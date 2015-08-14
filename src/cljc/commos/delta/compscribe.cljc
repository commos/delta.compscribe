(ns commos.delta.compscribe
  (:require [commos.delta :as delta]
            [commos.delta.cache :as dc]
            [commos.service :as service]
            [#?(:clj clojure.core.async
                :cljs cljs.core.async) :refer [chan close!
                                               <! >!
                                               put!
                                               alts!
                                               pipe
                                               #?@(:clj [go go-loop])
                                               tap untap] :as a]
            [clojure.walk :refer [prewalk postwalk]])
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go-loop go]])))

(defprotocol ClosingMix
  "Implementation detail, subject to change."
  ;; Mix in core.async is designed to close only when the target
  ;; channel closes. This leaves no way to determine when all sources
  ;; have been consumed and close the target channel as a
  ;; consequence. This design provides a mix that closes immediately
  ;; when there are no more sources or when the sources have been
  ;; drained.
  (mix-in [this ch] "Implementation detail, subject to change.")
  (mix-out [this ch] "Implementation detail, subject to change."))

(defn- closing-mix
  [target init-chs]
  {:pre [(vector? init-chs)]}
  (let [change (chan)
        channels (atom (conj init-chs))
        m (reify ClosingMix
            (mix-in [_ ch]
              (swap! channels conj ch)
              (put! change true))
            (mix-out [_ ch]
              (swap! channels (comp vec
                                    (partial remove #{ch})))
              (put! change true)))]
    (go-loop []
      (let [chs @channels]
        (if (seq @channels)
          (let [[v ch] (alts! (conj chs change))]
            (cond (identical? change ch)
                  (recur)
                  
                  (nil? v)
                  (do (mix-out m ch)
                      (recur))
                  
                  :else
                  (if (>! target v)
                    (recur))))
          (do
            (close! change)
            (close! target)))))
    m))

(defn- extract-hooks
  "Return a map of required subscriptions and unsubscriptions with the
  following keys:

  :subs-one ks->val - subscribe hook at ks with val
  :subs-many ks->vals - subscribe hooks at ks with vals
  :unsubs [ks+] - unsubscribe subscriptions at ks

  Subs and unsubs may overlap, unsubs are assumed to be applied
  first."
  [spec-map delta]
  (loop [[delta & deltas] (delta/unpack delta)
         subs {}
         adjusted-deltas []]
    (if delta
      (let [[op ks new-val] delta
            hook (get-in spec-map ks)]
        ;; TODO: refactor else recurs
        (case op
          :is
          (if hook
            (cond (map? hook)
                  (if (map? new-val)
                    (recur (concat (delta/implied-deltas {} delta)
                                   deltas)
                           subs
                           adjusted-deltas)
                    (let [msg (str "Can't assert non-map at "
                                   (pr-str ks))]
                      (throw
                       #?(:clj
                          (IllegalArgumentException. msg)
                          :cljs
                          msg))))

                  (vector? hook)
                  (recur deltas
                         (-> subs
                             (assoc-in [(if (coll? new-val)
                                          :subs-many
                                          :subs-one) ks]
                                       new-val)
                             (update-in [:unsubs] conj ks))
                         adjusted-deltas)

                  :else
                  (let [msg (str "Invalid spec"  (pr-str hook))]
                    (throw
                     #?(:clj
                        (IllegalArgumentException. msg)
                        :cljs
                        msg))))
            (recur deltas
                   subs
                   (conj adjusted-deltas delta)))
          :in (if hook
                (recur deltas
                       (update-in subs [:subs-many ks] into new-val)
                       adjusted-deltas)
                (recur deltas
                       subs
                       (conj adjusted-deltas delta)))
          :ex (if hook
                (recur deltas
                       (update-in subs [:unsubs] into
                                  (map (partial conj ks) new-val))
                       (conj adjusted-deltas delta))
                (recur deltas
                       subs
                       (conj adjusted-deltas delta)))))
      [subs (delta/pack adjusted-deltas)])))

(defn- compscribe*
  [outer-target ch-in compscribe-service spec-map]
  (let [;; Once intercepted, events need to go through target-mix so
        ;; that mix-ins and mix-outs have synchronous effects
        target (chan)
        target-mix (closing-mix outer-target [target])
        subs (volatile! {})
        do-sub (fn [ks id many?]
                 (let [ks' (cond-> ks
                             many? (conj id))
                       xch (chan 1 (delta/nest ks'))]
                   (service/request compscribe-service
                                    [(get-in spec-map ks) id]
                                    xch)
                   (vswap! subs assoc ks' xch)
                   ;; xch will be closed by the subscribed-composition
                   (mix-in target-mix xch)))]
    (go-loop []
      (if-some [delta (<! ch-in)]
        (let [[{:keys [subs-one subs-many unsubs]} delta :as dbg]
              (extract-hooks spec-map delta)]
          (doseq [[ks xch] (filter (fn [[ks v]]
                                     (some (fn [uks]
                                             (= uks (take (count uks) ks)))
                                           unsubs))
                                   @subs)]
            (service/cancel compscribe-service xch)
            (mix-out target-mix xch)
            (vswap! subs dissoc ks))

          (when delta ;; (it is possible that all deltas got eaten up)
            (>! target delta)) ;; Block until evt is put so that
                               ;; subscriptions are put after and
                               ;; unsubscriptions are in effect
          
          (doseq [[ks id] subs-one]
            (do-sub ks id false))

          (doseq [[ks ids] subs-many
                  id ids]
            (do-sub ks id true))
          (recur))
        (do
          (close! target) ;; implicit mix-out
          (doseq [xch (vals @subs)]
            (service/cancel compscribe-service xch)))))))

(defn- swap-out!
  "Atomically dissocs k in atom, returns k"
  [atom k]
  (-> atom
      (swap! (fn [m]
               (-> m
                   (dissoc k)
                   (vary-meta assoc ::swapped-out (get m k)))))
      (meta)
      ::swapped-out))

(defn- on-close-pipe
  "Like pipe, but invokes on-close when source closes."
  [source target on-close]
  (let [watch-ch (chan 1 (fn [rf]
                           (completing rf
                                       (fn [result]
                                         (on-close)
                                         (rf result)))))]

    (pipe source watch-ch)
    (pipe watch-ch target)))

(defn- on-close-source
  "Pipes a channel to target and returns it, on-close is invoked when
  it is closed."
  [target on-close]
  (doto (chan)
    (on-close-pipe target on-close)))

(defn- compscribe-service
  [source-service]
  (let [subscriptions (atom {})]
    (reify
      service/IService
      (request [this spec target]
        (let [[spec id] spec
              [endpoint spec-map] spec
              subs-target (on-close-source target
                                           #(service/cancel this target))]
          (if (empty? spec-map)
            ;; OPT: If there is nothing to compscribe, directly reach
            ;; through to the source-service:
            (do
              (service/request source-service [endpoint id] subs-target)
              (swap! subscriptions assoc target subs-target))
            (let [ch-in (chan)]
              (compscribe* subs-target
                           ch-in
                           (service/cached this)
                           spec-map)
              (service/request source-service [endpoint id] ch-in)
              (swap! subscriptions assoc target ch-in)))))
      (cancel [this target]
        (when-let [ch (swap-out! subscriptions target)]
          (service/cancel source-service ch))))))

(defn compscriber
  "Return a service for requesting composite streams of multiple
  commos.delta streams.

  Requests can be made with

  [spec id?]

  as the request-spec argument.

  spec is a nested datastructure describing a composition:
  
  [endpoint (spec-map | spec)?]
  (Another spec may only be used directly in a spec if endpoint
  streams a set.)

  endpoint may be any endpoint recognized by the service you pass
  where compscriber makes requests with 

  [endpoint id] as request-spec argument.

  spec-map is a map {(key (spec-map | spec))+}
  
  Deltas are transformed and trigger requests according to these
  rules:

  1. If a value streamed at key is not a set, it is requested and
  transformed to assert at key.

  2. If a value streamed at key is a set, its elements are requested
  at the specified endpoint and are transformed to assert a map
  {(id streamed-val)+} at key.

  Example spec:

  [:users {:user/orders [:orders {:order/invoice [:invoices]
                                  :order/item [:items]}]
           :user/cart [:items]}]

  Subscriptions are coordinated, meaning nested subscriptions are
  streamed always after a delta that started them and never after a
  delta that stopped them.

  Caching:

  By default, the service caches on subscription identifiers both made
  by the user and internally, using sum caching (see
  commos.delta.sum-cache).  Thus compscribe streams only :is deltas
  with the whole compscribed value.  Nested values streamed by equal
  specs are (memory) identical.

  Use the :cache opt for different caching behavior.

  Depending on your specs, the service may subscribe equal specs at
  the source.  Passing a cached service is thus recommended.

  Refer to commos.delta.cache and commos.service to learn more about
  service caching.

  Opts:

  :cache - A function transforming a service."
  [source & {:keys [cache] :as opts
             :or {cache dc/sum-cache}}]
  (let [service (-> source
                    (compscribe-service)
                    (cache))]
    (reify
      service/IService
      (request [_ spec target]
        (service/request service 
                         spec
                         target))
      (cancel [_ target]
        (service/cancel service target)))))

(defn compscribe
  "Internally creates a compscribe service and makes the subscription
  at specifier.  Useful for testing, uses default cache settings,
  returns a function to end subscription."
  {:arglists '([target-ch service spec id])}
  ([target-ch service spec id]
   (let [service (compscriber service)]
     (service/request service [spec id] target-ch)
     #(service/cancel service target-ch)))
  ([target-ch subs-fn unsubs-fn spec id]
   (compscribe target-ch
               (let [chs (atom {})]
                 (reify
                   service/IService
                   (request [this [endpoint id] target]
                     (let [ch-in (subs-fn endpoint id)]
                       (swap! chs assoc target ch-in)
                       (on-close-pipe ch-in target
                                      #(service/cancel this target))))
                   (cancel [_ ch]
                     (some-> (swap-out! chs ch)
                             (unsubs-fn)))))
               spec id)))
