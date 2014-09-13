(defproject raftcj "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
  :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
    [org.clojure/clojurescript "0.0-2311"]]
  :plugins [[lein-cljsbuild "1.0.4-SNAPSHOT"]]
  :source-paths ["src"]
  :main ^:skip-aot raftcj.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :cljsbuild {
    :builds [{:id "raftcj"
              :source-paths ["src"]
              :compiler {
                :output-to "raftcj.js"
                :output-dir "out"
                :optimizations :none
                :source-map true}}]})
