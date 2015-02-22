(ns commos.delta.compscribe
  (:require [commos.delta :as delta]
            #+clj [clojure.core.async :refer [chan close!
                                              <! >!
                                              put!
                                              alts!
                                              go go-loop]]
            #+cljs [cljs.core.async :refer [chan close!
                                            <! >!
                                            put!
                                            alts!]]
            [clojure.walk :refer [prewalk postwalk]]
            [commos.shared.core :refer [flatten-keys]])
  #+cljs (:require-macros [cljs.core.async.macros :refer [go-loop go]]))

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

(defn- conform-spec
  "Recursively transform spec [endpoint hooks] [endpoint
  flattened-keys-hooks flattened-keys-hooks-grouped-by-pks hooks].
  The resulting structure provides fast lookups required during live
  dispatch."
  [spec]
  (let [mark-spec #(vary-meta % assoc ::spec? true)]
    (->> (mark-spec spec)
         (prewalk (fn [form]
                    (if (::spec? (meta form))
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
                     (if (::spec? (meta form))
                       (let [[endpoint specs] form]
                         [endpoint specs (group-by-pks specs)])
                       form))))))

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

  Pass nil a new-val to only get unsubscriptions."
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
          :is (if hook
                (recur deltas
                       (-> subs
                           (assoc-in [:subs-one ks] new-val)
                           (update-in [:unsubs] conj ks))
                       adjusted-deltas)
                (if (associative? new-val)
                  (let [[subs new-val]
                        (nested-subs subs deep-hooks ks new-val)]
                    (recur deltas
                           subs
                           (cond-> adjusted-deltas
                             (seq new-val)
                             (conj
                              (delta/summable-delta [:is ks new-val])))))
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
                                         ks
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
  ;; NOTE: before replacing subs-fn and unsubs-fn with a protocol I want
  ;; to reconsider whether they should return channels
  [outer-target subs-fn unsubs-fn
   [endpoint direct-hooks deep-hooks :as conformed-spec] id]
  (let [;; Once intercepted, events need to go through target-mix so
        ;; that mix-ins and mix-outs have synchronous effects
        target (chan)
        target-mix (closing-mix outer-target [target])
        subs (atom {})
        do-sub (fn [ks id many?]
                 (let [xch (chan 1 (delta/nest (cond-> ks
                                                 many? (conj id))))]
                   (swap! subs assoc ks
                          [xch (compscribe* xch
                                            subs-fn
                                            unsubs-fn
                                            (get direct-hooks ks)
                                            id)])
                   ;; xch will be closed by the subscribed-composition
                   (mix-in target-mix xch)))
        ch-in (subs-fn endpoint id)
        
        transport
        (go-loop []
          (if-some [delta (<! ch-in)]
            (let [[{:keys [subs-one subs-many unsubs]} delta]
                  (extract-hooks conformed-spec delta)]
              
              (doseq [[ks [xch unsubs-fn]] (keep (partial find @subs) unsubs)]
                (unsubs-fn)
                (mix-out target-mix xch)
                (swap! subs dissoc ks))

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
              (doseq [[_ unsubs-fn] (vals @subs)]
                (unsubs-fn)))))]
    (fn [] (unsubs-fn ch-in))))

(defn compscribe
  "Asynchronously subscribes via subs-fn and unsubs-fn at one or more
  endpoints, combines received deltas so that they can be added to one
  combined value and puts them onto target-ch.

  Specify used endpoints and their desired nesting in spec so:

  [endpoint (spec-map | spec)?]
  
  root-endpoint may be any value recognized by subs-fn. 

  spec-map is a map {(key (spec-map | spec))+}

  Deltas are composed according to the following rules:

  1. If the id at key is not a collection, it is replaced with the
  deltas streamed from endpoint.

  2. If the id at key is a collection, it is replaced with deltas that
  add to a map {(id streamed-val)+} from endpoint.

  subs-fn is invoked with an endpoint and an id as arguments. It must
  return a unique core.async channel from which deltas can be taken.

  unsubs-fn may be invoked with the channel returned from subs-fn. It
  must close the channel and is not invoked if the channel is closed
  elsewhere.

  id is used to make an initial subscription at the root endpoint.

  Returns a function that may be used to end all made subscriptions
  and close target-ch."
  [target-ch subs-fn unsubs-fn spec id]
  (let [conformed-spec (conform-spec spec)]
    (compscribe* target-ch subs-fn unsubs-fn conformed-spec id)))
