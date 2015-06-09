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
  (let [block-foo (chan)
        [subs-fn unsubs-fn [subscriptions unsubscribable]]
        (simulate-api
         {"/foo/"
          {:deltas {0 [[:is #{0}]
                       block-foo]}}
          "/bar/"
          {:deltas {0 [[:is :bar 42]]}}})
        target (chan 1 delta/values)
        end-subscription (compscribe target subs-fn unsubs-fn
                                     ["/foo/" ["/bar/"]]
                                     0)]
    (test-async
     (test-within 1000
       (go
         (is (= (<! target)
                {0 {:bar 42}}))
         (close! block-foo))))))

(deftest compscribe-one
  (let [block-foo (chan)
        [subs-fn unsubs-fn [subscriptions unsubscribable]]
        (simulate-api
         {"/foo/"
          {:deltas {0 [[:is :bar 0]
                       block-foo]}}
          "/bar/"
          {:deltas {0 [[:is :baz 42]]}}})
        target (chan 1 delta/values)
        end-subscription (compscribe target subs-fn unsubs-fn
                                     ["/foo/" {:bar ["/bar/"]}]
                                     0)]
    (test-async
     (test-within 1000
       (go
         (is (= (<! target)
                {:bar {:baz 42}}))
         (close! block-foo))))))

(deftest compscribe-many
  (let [block-foo (chan)
        [subs-fn unsubs-fn [subscriptions unsubscribable]]
        (simulate-api
         {"/foo/"
          {:deltas {0 [[:in :bar 0]
                       block-foo]}}
          "/bar/"
          {:deltas {0 [[:is :baz 42]]}}})
        target (chan 1 delta/values)
        end-subscription (compscribe target subs-fn unsubs-fn
                                     ["/foo/" {:bar ["/bar/"]}]
                                     0)]
    (test-async
     (test-within 1000
       (go
         (is (= (<! target)
                {:bar {0 {:baz 42}}}))
         (close! block-foo))))))

(deftest compscribe-nested
  (let [block-foo (chan)
        block-bar (chan)
        [subs-fn unsubs-fn [subscriptions unsubscribable]]
        (simulate-api
         {"/foo/"
          {:deltas {0 [[:in :bar 0]
                       block-foo]}}
          "/bar/"
          {:deltas {0 [[:is :baz 0]
                       block-bar]}}
          "/baz/"
          {:deltas {0 [[:is 42]]}}})
        target (chan 1 delta/values)
        end-subscription (compscribe target subs-fn unsubs-fn
                                     ["/foo/" {:bar ["/bar/" {:baz ["/baz/"]}]}]
                                     0)]
    (test-async
     (test-within 1000
       (go
         (is (= (<! target)
                {:bar {0 {:baz 42}}}))
         (close! block-bar)
         (close! block-foo))))))

(deftest uncompscribe-root
  (let [foo-complete (chan)
        unsubs-bar (chan)
        [subs-fn unsubs-fn [subscriptions unsubscribable]] (simulate-api
                                           {"/foo/"
                                            {:deltas {0 [[:in 0]
                                                         [:ex 0]
                                                         foo-complete]}}
                                            "/bar/"
                                            {:deltas {0 [[:is :bar 42]]}
                                             :on-unsubs unsubs-bar}})
        target (chan)
        end-subscription (compscribe target subs-fn unsubs-fn
                                     ["/foo/" ["/bar/"]]
                                     0)]
    (test-async
     (test-within 1000
       (go
         (is (= 0 (<! unsubs-bar)))
         (close! foo-complete)
                  
         (is (empty? (reduce delta/add nil (<! (a/into [] target))))))))))

(deftest uncompscribe-one
  (let [foo-complete (chan)
        unsubs-bar (chan)
        [subs-fn unsubs-fn [subscriptions unsubscribable]]
        (simulate-api
         {"/foo/"
          {:deltas {0 [[:is :bar 0]
                       [:ex :bar]
                       foo-complete]}}
          "/bar/"
          {:deltas {0 [[:is :baz 42]]}
           :on-unsubs unsubs-bar}})
        target (chan)
        end-subscription (compscribe target subs-fn unsubs-fn
                                     ["/foo/" {:bar ["/bar/"]}]
                                     0)]
    (test-async
     (test-within 1000
       (go
         (is (= 0 (<! unsubs-bar)))
         (close! foo-complete)
         (is (empty? (reduce delta/add nil (<! (a/into [] target))))))))))

(deftest uncompscribe-many
  (let [foo-complete (chan)
        unsubs-bar (chan)
        [subs-fn unsubs-fn [subscriptions unsubscribable]]
        (simulate-api
         {"/foo/"
          {:deltas {0 [[:in :bar 0]
                       [:ex :bar]
                       foo-complete]}}
          "/bar/"
          {:deltas {0 [[:is :baz 42]]}
           :on-unsubs unsubs-bar}})
        target (chan)
        end-subscription (compscribe target subs-fn unsubs-fn
                                     ["/foo/" {:bar ["/bar/"]}]
                                     0)]
    (test-async
     (test-within 1000
       (go
         (is (= 0 (<! unsubs-bar)))
         (close! foo-complete)
         (is (empty? (reduce delta/add nil (<! (a/into [] target))))))))))
