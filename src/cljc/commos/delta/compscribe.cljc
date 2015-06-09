(ns commos.delta.compscribe
  (:require [commos.delta :as delta]
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
      (let [[op ks new-val] (delta/diagnostic-delta delta)
            hook (get direct-hooks ks)]
        (case op
          :is (let [[new-val] new-val]
                (if hook
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
                                (delta/summable-delta [:is ks [new-val]])))))
                    (recur deltas
                           subs
                           (conj adjusted-deltas delta)))))
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

(defprotocol IStream
  (subscribe [this spec ch]
    "Stream commos deltas on core.async channel ch.  ch is expected to
    be used with only one subscription.")
  (cancel [this ch]
    "Asynchronously end the subscription associated with ch and close
    ch."))

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
                   (subscribe compscribe-service
                              [(get direct-hooks ks) id]
                              xch)
                   (vswap! subs assoc ks' xch)
                   ;; xch will be closed by the subscribed-composition
                   (mix-in target-mix xch)))
        ch-in (chan)]
    (subscribe source-service [endpoint id] ch-in)
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
            (cancel compscribe-service xch)
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
            (cancel compscribe-service xch)))))
    (fn [] (cancel source-service ch-in))))

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

(defn- caching
  [service cached-service]
  (vary-meta service assoc ::cached-service cached-service))

(defn- cached
  [service]
  (::cached-service (meta service) service))

(defn- compscribe-service
  [source-service]
  (let [subscriptions (atom {})]
    (reify
      IStream
      (subscribe [this spec target]
        (let [[spec id] spec
              [endpoint direct-hooks deep-hooks :as spec] (compile-spec spec)
              subs-target (on-close-source target #(cancel this target))]
          (if (and (empty? direct-hooks)
                   (empty? deep-hooks))
            ;; OPT: If there is nothing to compscribe, directly reach
            ;; through to the source-service:
            (do
              (subscribe source-service [endpoint id] subs-target)
              (swap! subscriptions assoc target
                     #(cancel source-service subs-target)))
            (do
              (swap! subscriptions assoc target
                     (compscribe* subs-target
                                  source-service
                                  (cached this)
                                  spec
                                  id))))))
      (cancel [this target]
        (when-let [unsubs-fn (swap-out! subscriptions target)]
          (unsubs-fn))))))

(defn- caching-mult
  [ch {:keys [accumulate
              mode]
       :or {mode :value}}]
  (let [tap-ch (chan)
        chs (atom #{})
        dctr (atom nil)
        dchan (chan 1)
        done (fn [_] (when (zero? (swap! dctr dec))
                       (put! dchan true)))
        m (reify
            a/Mux
            (muxch* [_] ch)
            a/Mult
            (tap* [_ ch close?]
              (if close?
                (put! tap-ch ch)
                (throw (UnsupportedOperationException.
                        "close?=false not supported"))))
            (untap* [_ ch]
              (swap! chs disj ch))
            (untap-all* [_] (throw (UnsupportedOperationException.))))]
    (go-loop [cache nil
              receiving? true]
      (let [[v port] (alts! (cond-> [tap-ch]
                              receiving? (conj ch)) :priority true)]
        (condp identical? port
          tap-ch
          (do (when (and (or (nil? cache)
                             (>! v cache)))
                (if receiving?
                  (swap! chs conj v)
                  (close! v)))
              (recur cache
                     receiving?))
          ch
          (if (nil? v)
            (do
              (run! close! @chs)
              (reset! chs nil)
              (recur cache
                     false))
            (let [cache (accumulate cache v)
                  v (case mode
                      :cache cache
                      :value v)]
              (when-let [chs (seq @chs)]
                (reset! dctr (count chs))
                (doseq [ch chs]
                  (when-not (put! ch v done)
                    (a/untap* m ch)))
                (<! dchan))
              (recur cache
                     true))))))
    m))

(defn- cached-service
  [service opts]
  (let [subscribe-ch (chan)
        cancel-ch (chan)]
    (go-loop [subs {}
              chs {}]
      (let [[msg port] (alts! [subscribe-ch
                               cancel-ch])]
        (condp identical? port
          subscribe-ch
          (let [[this spec target] msg
                [m :as cache]
                (or (get subs spec)
                    (let [ch-in (chan)
                          m (caching-mult ch-in opts)]
                      (subscribe (caching service this)
                                 spec
                                 ch-in)
                      [m 0 ch-in]))
                target-step (on-close-source target
                                             #(cancel this target))]
            (tap m target-step)
            (recur (assoc subs spec (update cache 1 inc))
                   (assoc chs target [spec target-step])))
          cancel-ch
          (let [[this target] msg]
            (if-let [[spec target-step] (get chs target)]
              (let [[m m-chs ch-in :as cache] (get subs spec)
                    m-chs (dec m-chs)
                    chs (dissoc chs target)]
                (untap m target-step)
                (close! target-step)
                (if (zero? m-chs)
                  (do
                    (cancel service ch-in)
                    (recur (dissoc subs spec)
                           chs))
                  (recur (assoc subs spec (assoc cache 1 m-chs))
                         chs)))
              (recur subs
                     chs))))))
    (reify
      IStream
      (subscribe [this spec target]
        (put! subscribe-ch [this spec target]))
      (cancel [this target]
        (put! cancel-ch [this target])))))

(defn- hybrid-cache
  "Caches on endpoints.  Sends the current sum to a new subscriber,
  continues with deltas."
  [service]
  (cached-service service {:accumulate
                           (fn [cache v]
                             (update (or cache [:is]) 1
                                     delta/add v))}))

(defn- sum-cache
  "Caches on endpoints.  Sends the current sum to a new subscriber,
  continues with sums."
  [service]
  (cached-service service {:accumulate
                           (fn [cache v]
                             (update (or cache [:is]) 1
                                     delta/add v))
                           :mode :cache}))

(defn compscriber
  "Return a service that transforms subscriptions it makes at source,
  an IStream implementation, so that they add up to one map.

  When making a subscription, specify endpoints you want to use and
  their desired nesting in spec so:

  [endpoint (spec-map | spec)?]
  (Another spec may only be used directly in a spec if endpoint
  streams a set.)
  
  endpoint may be any value recognized by the source you pass.

  spec-map is a map {(key (spec-map | spec))+}

  1. If the value streamed at key is not a set, it is subscribed at
  the specified endpoint and the subscription is transformed to assert
  at key.

  2. If the value streamed at key is a set, its elements are
  subscribed at the specified endpoint and are transformed to assert a
  map {(set-elem streamed-val)+} at key.

  Service makes subscriptions at source, with

  [endpoint (value | set-elem)] as spec argument.

  You can make subscriptions at service with

  [spec id?]

  as the spec argument.

  Example spec:

  [\"users\" {:user/orders [\"orders\" {:order/invoice [\"invoices\"]
                                        :order/item [\"items\"]}]
              :user/cart [\"items\"]}]

  Subscriptions are synchronized, meaning nested subscriptions are
  streamed always after a delta that started them and never after a
  delta that stopped them.

  By default, the service caches on subscription identifiers both made
  by the user and internally, using sum caching.  Thus compscribe
  streams only :is deltas with the whole compscribed value.  Nested
  values streamed by equal specs are (memory) identical.

  Depending on your specs, the service may subscribe equal specs at
  the source.  Passing a cached service is recommended.

  Use the :cache opt for different caching behavior.

  Opts:

  :cache - A function transforming a service.  

  At the time, :batch deltas are not recomposed to a whole if they
  start nested subscriptions.  This means that a partial :batch delta
  may be streamed before subscriptions derived from it are streamed."
  [source & {:keys [cache] :as opts
             :or {cache sum-cache}}]
  (let [service (-> source
                    (compscribe-service)
                    (cache))
        compile-spec (memoize compile-spec)]
    (reify
      IStream
      (subscribe [_ spec target]
        (subscribe service 
                   (update spec 0 compile-spec)
                   target))
      (cancel [_ target]
        (cancel service target)))))

(defn compscribe
  "Internally creates a compscribe service and makes the subscription
  at specifier.  Useful for testing, uses default cache settings,
  returns a function to end subscription."
  {:arglists '([target-ch service spec id])}
  ([target-ch service spec id]
   (let [service (compscriber service)]
     (subscribe service [spec id] target-ch)
     #(cancel service target-ch)))
  ([target-ch subs-fn unsubs-fn spec id]
   (compscribe target-ch
               (let [chs (atom {})]
                 (reify
                   IStream
                   (subscribe [this [endpoint id] target]
                     (let [ch-in (subs-fn endpoint id)]
                       (swap! chs assoc target ch-in)
                       (on-close-pipe ch-in target
                                      #(cancel this target))))
                   (cancel [_ ch]
                     (some-> (swap-out! chs ch)
                             (unsubs-fn)))))
               spec id)))
