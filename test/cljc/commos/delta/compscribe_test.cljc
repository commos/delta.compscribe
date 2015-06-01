(ns commos.delta.compscribe-test
  (:require #?@(:clj [[clojure.test :refer :all]
                      [clojure.core.async
                       :refer [go go-loop
                               chan close!
                               >! <!
                               put!
                               timeout]
                       :as a]]
                :cljs [[cljs.test :refer-macros [is deftest]]
                       [cljs.core.async
                       :refer [chan close!
                               >! <!
                               put! take!
                               timeout]
                       :as a]])
            [commos.delta.compscribe-test.helpers :refer [simulate-api
                                                          test-within
                                                          test-async]]
            [commos.delta.compscribe :refer [compscribe]]
            [commos.delta :as delta])
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go go-loop]])))

(deftest compscribe-root
  (let [[subs-fn unsubs-fn [subscriptions unsubscribable]]
        (simulate-api
         {"/foo/"
          {:deltas {0 [[:is #{0}]]}}
          "/bar/"
          {:deltas {0 [[:is :bar 42]]}}})
        target (chan)
        end-subscription (compscribe target subs-fn unsubs-fn
                                     ["/foo/" ["/bar/"]]
                                     0)]
    (test-async
     (test-within 1000
       (go
         (let [evts (<! (a/into [] target))]
           (is (= evts
                  [[:is [0 :bar] 42]]))))))))

(deftest compscribe-one
  (let [[subs-fn unsubs-fn [subscriptions unsubscribable]]
        (simulate-api
         {"/foo/"
          {:deltas {0 [[:is :bar 0]]}}
          "/bar/"
          {:deltas {0 [[:is :baz 42]]}}})
        target (chan)
        end-subscription (compscribe target subs-fn unsubs-fn
                                     ["/foo/" {:bar ["/bar/"]}]
                                     0)]
    (test-async
     (test-within 1000
       (go
         (let [evts (<! (a/into [] target))]
           (is (= evts
                  [[:is [:bar :baz] 42]]))))))))

(deftest compscribe-many
  (let [[subs-fn unsubs-fn [subscriptions unsubscribable]]
        (simulate-api
         {"/foo/"
          {:deltas {0 [[:in :bar 0]]}}
          "/bar/"
          {:deltas {0 [[:is :baz 42]]}}})
        target (chan)
        end-subscription (compscribe target subs-fn unsubs-fn
                                     ["/foo/" {:bar ["/bar/"]}]
                                     0)]
    (test-async
     (test-within 1000
       (go
         (let [evts (<! (a/into [] target))]
           (is (= evts
                  [[:is [:bar 0 :baz] 42]]))))))))

(deftest compscribe-nested
  (let [[subs-fn unsubs-fn [subscriptions unsubscribable]]
        (simulate-api
         {"/foo/"
          {:deltas {0 [[:in :bar 0]]}}
          "/bar/"
          {:deltas {0 [[:is :baz 0]]}}
          "/baz/"
          {:deltas {0 [[:is 42]]}}})
        target (chan)
        end-subscription (compscribe target subs-fn unsubs-fn
                                     ["/foo/" {:bar ["/bar/" {:baz ["/baz/"]}]}]
                                     0)]
    (test-async
     (test-within 1000
       (go
         (let [r (reduce delta/add nil (<! (a/into [] target)))]
           (is (= r
                  {:bar {0 {:baz 42}}}))))))))

(deftest uncompscribe
  (let [foo-complete (chan)
        bar-block (chan)
        [subs-fn unsubs-fn [subscriptions unsubscribable]] (simulate-api
                                           {"/foo/"
                                            {:deltas {0 [[:in 0]
                                                         [:ex 0]
                                                         foo-complete]}}
                                            "/bar/"
                                            {:deltas {0 [bar-block
                                                         [:is :bar 42]]}}})
        target (chan)
        end-subscription (compscribe target subs-fn unsubs-fn
                                     ["/foo/" ["/bar/"]]
                                     0)]
    (test-async
     (test-within 1000
       (go
         (>! foo-complete true)
         (close! bar-block)
         (is (empty? (reduce delta/add nil (<! (a/into [] target))))))))))
