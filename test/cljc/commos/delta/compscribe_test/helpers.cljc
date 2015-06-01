(ns commos.delta.compscribe-test.helpers
  (:require [#?(:clj clojure.core.async.impl.protocols
                :cljs cljs.core.async.impl.protocols) :refer [Channel]]
            #?@(:clj [[clojure.core.async :refer [chan close! put!
                                                  <! >! <!!
                                                  go-loop go
                                                  alts! timeout]]]
                :cljs [[cljs.core.async :refer [chan close! put!
                                                <! >! take!
                                                timeout alts!]]
                       [cljs.test]]))
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go-loop go]]
                            [cljs.test :refer [async]])))

(defn- chan?
  [x]
  (satisfies? Channel x))

(defn simulate-api
  "Helper to simulate different API endpoints via endpoints, a map
  endpoint->spec, where spec consists of

  :deltas - a map value->deltas.  Channels may be placed instead of
            deltas and block successive deltas to simulate coordination 
            scenarios.
                                                                               
  :unsubs-mode - either :nop (default) or :close.  Deltas are put on
                 the returned channels \"as is\".

  Returns [subs-fn unsubs-fn channels] where subs-state is an atom
  endpoint->value->channels, for inspection."
  [endpoints]
  (let [subs (atom {})
        unsubscribable (atom #{})]
    [(fn [endpoint value]
       (let [ch (chan)]
         (swap! subs assoc ch [endpoint value])
         (when (= :close (get-in endpoints [endpoint :unsubs-mode]))
           (swap! unsubscribable conj ch ))
         (go-loop [[x & xs] (get-in endpoints
                                    [endpoint :deltas value])]
           (if x
             (do
               (if (chan? x)
                 (<! x)
                 (>! ch x))
               (recur xs))
             (close! ch)))
         ch))
     (fn [ch]
       (when (@unsubscribable ch)
         (close! ch)
         (swap! unsubscribable disj ch))
       (let [[endpoint id] (get @subs ch)]
         (when-let [on-unsubs (get-in endpoints [endpoint :on-unsubs])]
           (put! on-unsubs id))))
     [subs unsubscribable]]))

(defn test-within
  "Throws if ch does not close or produce a value within ms. Returns a
  channel from which the value can be taken."
  [ms ch]
  (go (let [t (timeout ms)
            [v ch] (alts! [ch t])]
        (assert (not= ch t)
                (str "Test should have finished within " ms "ms."))
        v)))

(defn test-async
  "Asynchronous test awaiting ch to produce a value or close."
  [ch]
  #?(:clj
     (<!! ch)
     :cljs
     (async done
       (take! ch (fn [_] (done))))))
