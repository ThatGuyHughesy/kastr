(ns kastr.unit.core
  (:require [clojure.test :refer :all]
            [kastr.core :refer :all]))

(def test-configuration {:application-id "kafka-streams-test-1"
                         :bootstrap-servers "localhost:9092"})

(def test-properties {"application.id" "kafka-streams-test-1"
                      "bootstrap.servers" "localhost:9092"})

(deftest ^:unit test-configuration-mapping-value
  (testing "with :application-id"
    (is (= "test-app-id"
           (configuration-mapping-value :application-id "test-app-id"))))
  (testing "with :num-stream-threads"
    (is (= "1"
           (configuration-mapping-value :num-stream-threads 1))))
  (testing "with :auto-offset-reset"
    (is (= "earliest"
           (configuration-mapping-value :auto-offset-reset :earliest)))))

(deftest ^:unit test-configuration-mappings
  (testing "with :application-id"
    (is (= "application.id"
           (configuration-mappings :application-id))))
  (testing "with :num-stream-threads"
    (is (= "num.stream.threads"
           (configuration-mappings :num-stream-threads)))))

(deftest ^:unit test-kafka-streams-configuration
  (testing "with valid configuration"
    (is (= test-properties
           (kafka-streams-configuration test-configuration))))
  (testing "with valid configuration including optional"
    (is (= (assoc test-properties "num.stream.threads" "2")
           (-> (assoc test-configuration :num-stream-threads 2)
               (kafka-streams-configuration)))))
  (testing "with invalid configuration"
    (is (thrown? Exception (-> (dissoc test-configuration :application-id)
                               (kafka-streams-configuration))))))
