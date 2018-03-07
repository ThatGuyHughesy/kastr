(defproject kastr "0.1.0-SNAPSHOT"
  :description "Clojure wrapper for Kafka Streams"
  :url "https://github.com/ThatGuyHughesy/kastr"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :author "Conor Hughes <hello@conorhughes.me>"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.apache.kafka/kafka-streams "1.0.0"]
                 [com.taoensso/timbre "4.8.0"]
                 [prismatic/schema "1.1.7"]]
  :dev-dependencies [[lein-clojars "0.9.1"]]
  :profiles {:test {:dependencies [[org.apache.kafka/kafka-clients "1.0.0" :classifier "test"]
                                   [org.apache.kafka/kafka-streams "1.0.0" :classifier "test"]]}})