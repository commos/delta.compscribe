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
            [clojure.walk :refer [prewalk postwalk]]
            [commos.shared.core :refer [flatten-keys]])
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go-loop go]])))

(defn- vsplit-at
  "Like split-at, but for vectors."
  [n v]
  [(subvec v 0 n) (subvec v n)])

(defn- group-by-pks
  "Transform a flattened map ks->v into a map pks->[[rks ks v]+] where
  pks is a partial ks and rks the complementary rest.  E. g.:
  {[:foo :bar] 42} becomes {[:foo] ([[:bar] [:foo :bar]
  42]), [] ([[:foo :bar] [:foo :bar] 42])}."
  [m]
  (reduce-kv (fn [acc ks v]
               (reduce (fn [acc [pks rks]]
                         (update acc pks conj [rks ks v]))
                       acc
                       (map #(vsplit-at % ks)
                            (range (count ks)))))
             {}
             m))

(defn- compile-spec
  "Recursively transform spec [endpoint hooks] [endpoint
  flattened-keys-hooks flattened-keys-hooks-grouped-by-pks hooks].
  The resulting structure provides fast lookups required during live
  dispatch.  Returns compiled spec untransformed."
  [spec]
  (let [spec? #(::spec (meta %))
        mark-spec #(vary-meta % assoc ::spec true)]
    (cond-> spec
      (not (spec? spec))
      (->> (mark-spec)
           (prewalk (fn [form]
                      (if (spec? form)
                        (let [[endpoint specs] form]
                          (mark-spec
                           (if (vector? specs)
                             [endpoint {[] (mark-spec specs)}]
                             (let [specs (flatten-keys specs)]
                               [endpoint (zipmap (keys specs)
                                                 (map mark-spec
                                                      (vals specs)))]))))
                        form)))
           (postwalk (fn [form]
                       (if (spec? form)
                         (let [[endpoint specs] form]
                           (conj form (group-by-pks specs)))
                         form)))))))

(defn- dissoc-in
  ;; from org.clojure/core.incubator, copy & pasted due to lack of
  ;; clojurescript support
  "Dissociates an entry from a nested associative structure returning a new
  nested structure. keys is a sequence of keys. Any empty maps that result
  will not be present in the new structure."
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))

(defn- nested-subs
  "Extract necessary subscriptions/unsubscriptions implied by updating
  the new-val at ks.  Returns [subs new-val] where new-val has the
  subscribed ids removed and subs has the extracted
  subscriptions/unsubscriptions merged onto it.

  Pass nil as new-val to only get unsubscriptions."
  [subs deep-hooks ks new-val]
  (reduce (fn [[subs new-val] [rks ks _]]
            (if-let [val (get-in new-val rks)]
              [(if (coll? val)
                 (update-in subs [:subs-many ks] into val)
                 (-> subs
                     (assoc-in [:subs-one ks] val)
                     (update-in [:unsubs] conj ks)))
               (dissoc-in new-val ks)]
              [(update-in subs [:unsubs] conj ks)
               new-val]))
          [subs new-val]
          (get deep-hooks ks)))

(defn- extract-hooks
  "Return a map of required subscriptions and unsubscriptions with the
  following keys:

  :subs-one ks->val - subscribe hook at ks with val
  :subs-many ks->vals - subscribe hooks at ks with vals
  :unsubs [ks+] - unsubscribe subscriptions at ks

  Subs and unsubs may overlap, unsubs are assumed to be applied
  first."
  [[_ direct-hooks deep-hooks :as conformed-spec] delta]
  (loop [[delta & deltas] (delta/unpack delta)
         subs {}
         adjusted-deltas []]
    (if delta
      (let [[op ks new-val] delta
            hook (get direct-hooks ks)]
        (case op
          :is (if hook
                (recur deltas
                       (-> subs
                           (assoc-in [(if (set? new-val)
                                        :subs-many
                                        :subs-one) ks]
                                     new-val)
                           (update-in [:unsubs] conj ks))
                       adjusted-deltas)
                (if (map? new-val)
                  (let [[subs new-val]
                        (nested-subs subs deep-hooks ks new-val)]
                    (recur deltas
                           subs
                           (cond-> adjusted-deltas
                             (seq new-val)
                             (conj
                              [:is ks new-val]))))
                  (recur deltas
                         subs
                         (conj adjusted-deltas delta))))
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
                       (update-in subs [:unsubs] into
                                  (mapcat
                                   (fn [k]
                                     (let [ks (conj ks k)]
                                       (if (contains? direct-hooks ks)
                                         [ks]
                                         (if-let [nested (get deep-hooks ks)]
                                           (map second nested)))))
                                   new-val))
                       (conj adjusted-deltas delta)))))
      [subs (delta/pack adjusted-deltas)])))

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

(defn- compscribe*
  [outer-target source-service compscribe-service
   [endpoint direct-hooks deep-hooks :as conformed-spec] id]
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
                                    [(get direct-hooks ks) id]
                                    xch)
                   (vswap! subs assoc ks' xch)
                   ;; xch will be closed by the subscribed-composition
                   (mix-in target-mix xch)))
        ch-in (chan)]
    (service/request source-service [endpoint id] ch-in)
    (go-loop []
      (if-some [delta (<! ch-in)]
        (let [[{:keys [subs-one subs-many unsubs]} delta]
              (extract-hooks conformed-spec delta)

              subs-by-pks (group-by-pks @subs)]
          (doseq [[ks xch]
                  (->> unsubs
                       (mapcat (fn [ks]
                                 (concat (->> (get subs-by-pks ks)
                                              (map rest))
                                         (some-> (find @subs ks)
                                                 (vector)))))
                       distinct)]
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
            (service/cancel compscribe-service xch)))))
    (fn [] (service/cancel source-service ch-in))))

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
              [endpoint direct-hooks deep-hooks :as spec] (compile-spec spec)
              subs-target (on-close-source target
                                           #(service/cancel this target))]
          (if (and (empty? direct-hooks)
                   (empty? deep-hooks))
            ;; OPT: If there is nothing to compscribe, directly reach
            ;; through to the source-service:
            (do
              (service/request source-service [endpoint id] subs-target)
              (swap! subscriptions assoc target
                     #(service/cancel source-service subs-target)))
            (do
              (swap! subscriptions assoc target
                     (compscribe* subs-target
                                  source-service
                                  (service/cached this)
                                  spec
                                  id))))))
      (cancel [this target]
        (when-let [unsubs-fn (swap-out! subscriptions target)]
          (unsubs-fn))))))

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
                    (cache))
        compile-spec (memoize compile-spec)]
    (reify
      service/IService
      (request [_ spec target]
        (service/request service 
                         (update spec 0 compile-spec)
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
