(ns kastr.integration.map
  (:require [clojure.test :refer :all]
            [kastr.core :as ks]
            [kastr.integration.processor-topology-test-driver :refer :all]))

(def test-configuration
  {:kafka-streams {:application-id "kafka-streams-test-1"
                   :bootstrap-servers "localhost:9092"}
   :job {:input-topic "input"
         :output-topic "output"}})

(defn test-kafka-streams-topology
  [config stream-builder serde map-function stream-function]
  (let [{:keys [job]} config
        {:keys [input-topic output-topic]} job]
    (-> (ks/stream stream-builder [input-topic] serde serde nil nil)
        (map-function stream-function)
        (ks/to output-topic serde serde))))

(defn test-serde-e2e
  ([serde map-function stream-function message processd-v]
   (test-serde-e2e serde map-function stream-function message nil processd-v))
  ([serde map-function stream-function message processd-k processd-v]
   (let [stream-builder (ks/streams-builder)
         build-test-topology (test-kafka-streams-topology test-configuration stream-builder serde map-function stream-function)
         test-topology (ks/build stream-builder)
         stream-config (-> (:kafka-streams test-configuration)
                           (ks/kafka-streams-configuration)
                           (ks/streams-config))
         driver (processor-topology-test-driver stream-config test-topology)
         ser (ks/get-serializer serde)
         deser (ks/get-deserializer serde)
         input-topic (get-in test-configuration [:job :input-topic])
         output-topic (get-in test-configuration [:job :output-topic])
         _ (process driver input-topic nil message ser ser)
         processed-message (read-output driver output-topic deser deser)]
     (is (= processd-k (.key processed-message)))
     (is (= processd-v (.value processed-message))))))

(deftest ^:integration test-map
  (testing "map-v function"
    (test-serde-e2e ks/serde-long ks/map-v #(+ % 1) 1234567890 1234567891)
    (test-serde-e2e ks/serde-string ks/map-v #(str % " World") "Hello" "Hello World"))
  (testing "map-kv function"
    (test-serde-e2e ks/serde-integer ks/map-kv (fn [k v] [v v]) (int 1) 1 1)
    (test-serde-e2e ks/serde-float ks/map-kv (fn [k v] [v v]) (float 1) 1.0 1.0)))