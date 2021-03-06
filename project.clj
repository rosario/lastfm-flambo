(defproject spark-example "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opts ["-Xmx2G"]
  :auto-clean false
    :profiles {:dev
               {:aot [flambo.function flambo.kryo]}
             :provided
             {:dependencies
              [
               [org.apache.spark/spark-core_2.11 "2.0.1"]
               ]}
             :uberjar
               {:aot :all}}
    :dependencies [[org.clojure/clojure "1.8.0"]
                 [yieldbot/flambo "0.8.0"]
                 [clj-time "0.14.2"]
                 ])