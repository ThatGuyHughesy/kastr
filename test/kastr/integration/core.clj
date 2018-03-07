(ns kastr.integration.core
  (:require [clojure.test :refer :all]
            [kastr.core :refer :all]
            [kastr.integration.processor-topology-test-driver :refer :all]))

(def test-configuration
  {:kafka-streams {:application-id "kafka-streams-test-1"
                   :bootstrap-servers "localhost:9092"
                   :zookeeper-connect "localhost:2181"}
   :job {:input-topic "input"
         :output-topic "output"}})

(defn test-kafka-streams-topology
  [config serde stream-builder]
  (let [{:keys [job]} config
        {:keys [input-topic output-topic]} job
        message-stream (stream stream-builder [input-topic])]
    (to message-stream output-topic)))

(defn test-serde-e2e
  [serde message]
  (let [stream-builder (streams-builder)
        build-test-topology (test-kafka-streams-topology test-configuration serde stream-builder)
        test-topology (build stream-builder)
        stream-config (-> (:kafka-streams test-configuration)
                          (kafka-streams-configuration)
                          (streams-config))
        driver (processor-topology-test-driver stream-config test-topology)
        ser (get-serializer serde)
        deser (get-deserializer serde)
        input-topic (get-in test-configuration [:job :input-topic])
        output-topic (get-in test-configuration [:job :output-topic])]
    (process driver input-topic nil message ser ser)
    (is (= message
           (.value (read-output driver output-topic deser deser))))))

(deftest ^:integration test-serde
  (testing "with string serde"
    (test-serde-e2e serde-string "test-message"))
  (testing "with long serde"
    (test-serde-e2e serde-long 12345)))