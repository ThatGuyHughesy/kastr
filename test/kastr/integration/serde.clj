(ns kastr.integration.serde
  (:require [clojure.test :refer :all]
            [kastr.core :as ks]
            [kastr.integration.processor-topology-test-driver :refer :all]))

(def test-configuration
  {:kafka-streams {:application-id "kafka-streams-test-1"
                   :bootstrap-servers "localhost:9092"}
   :job {:input-topic "input"
         :output-topic "output"}})

(defn test-kafka-streams-topology
  [config stream-builder serde]
  (let [{:keys [job]} config
        {:keys [input-topic output-topic]} job]
    (-> (ks/stream stream-builder [input-topic] serde serde nil nil)
        (ks/to output-topic serde serde))))

(defn test-serde-e2e
  [serde message message-type]
  (let [stream-builder (ks/streams-builder)
        build-test-topology (test-kafka-streams-topology test-configuration stream-builder serde)
        test-topology (ks/build stream-builder)
        stream-config (-> (:kafka-streams test-configuration)
                          (ks/kafka-streams-configuration)
                          (ks/streams-config))
        driver (processor-topology-test-driver stream-config test-topology)
        ser (ks/get-serializer serde)
        deser (ks/get-deserializer serde)
        input-topic (get-in test-configuration [:job :input-topic])
        output-topic (get-in test-configuration [:job :output-topic])]
    (process driver input-topic nil message ser ser)
    (is (= message-type
           (type (.value (read-output driver output-topic deser deser)))))))

(deftest ^:integration test-serde
  (testing "string serde"
    (test-serde-e2e ks/serde-string "test-message" String))
  (testing "long serde"
    (test-serde-e2e ks/serde-long (long 12345) Long))
  (testing "short serde"
    (test-serde-e2e ks/serde-short (short 12345) Short))
  (testing "int serde"
    (test-serde-e2e ks/serde-integer (int 12345) Integer))
  (testing "float serde"
    (test-serde-e2e ks/serde-float (float 12345) Float))
  (testing "double serde"
    (test-serde-e2e ks/serde-double (double 12345) Double)))