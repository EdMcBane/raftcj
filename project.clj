(defproject raftcj "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
  :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
    [org.clojure/clojurescript "0.0-2311"]]
  :plugins [
    [lein-cljsbuild "1.0.4-SNAPSHOT"]
    [lein-simpleton "1.3.0"]
    [lein-pdo "0.1.1"]]
  :source-paths ["src" "target/classes"]
  :test-paths ["test" "target/test-classes"]
  :main ^:skip-aot raftcj.core
  :target-path "target/%s"
  :profiles {
    :uberjar {:aot :all}
    :dev {:plugins [[com.keminglabs/cljx "0.4.0"]]}}
  :cljsbuild {
    :builds [{:id "raftcj"
              :source-paths ["src" "test" "target/classes" "target/test-classes"]
              :compiler {
                :output-to "raftcj.js"
                :output-dir "out"
                :optimizations :none
                :source-map true}}]}
  :cljx {
    :builds [{  :source-paths ["src-cljx"]
                :output-path "target/classes"
                :rules :clj}
              { :source-paths ["src-cljx"]
                :output-path "target/classes"
                :rules :cljs}
              { :source-paths ["test-cljx"]
                :output-path "target/test-classes"
                :rules :clj}
              { :source-paths ["test-cljx"]
                :output-path "target/test-classes"
                :rules :cljs}]}
  :hooks [cljx.hooks leiningen.cljsbuild]
  :aliases {"demo" ["with-profile" "build"
                    "do" "clean,"
                    "cljx" "once,"
                    "run"]
            "build-auto" ["do" "clean,"
                          "cljx" "once,"
                          ["pdo"
                           "cljx" "auto,"
                           "cljsbuild" "auto,"
                           "simpleton" "8888"]]})
