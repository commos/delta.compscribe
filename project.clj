(defproject org.commos/delta.compscribe "0.1.9-SNAPSHOT"
  :description "Stream and combine commos.deltas from multiple endpoints"
  :url "http://github.com/commos/delta.compscribe"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0-RC1"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 
                 [org.commos/service "0.1.0"]
                 [org.commos/delta "0.3.0"]
                 [org.commos/delta.cache "0.1.0"]
                 [org.commos/shared "0.1.0"]]
  :source-paths ["src/cljc"]
  :profiles {:dev {:dependencies [[org.clojure/clojurescript "0.0-3269"]]
                   :plugins [[lein-cljsbuild "1.0.6"]]
                   :test-paths ["test/cljc"]
                   :cljsbuild
                   {:builds [{:id "test"
                              :source-paths ["test/cljc"
                                             "test/cljs"]
                              :compiler {:output-to "target/js/test.js"
                                         :output-dir "target/js"
                                         :optimizations :none
                                         :target :nodejs
                                         :cache-analysis true}}]}}})
