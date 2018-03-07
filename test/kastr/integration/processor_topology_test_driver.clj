(ns kastr.integration.processor-topology-test-driver
  (:import (org.apache.kafka.test ProcessorTopologyTestDriver)
           (org.apache.kafka.streams StreamsConfig Topology)))

(defn processor-topology-test-driver
  [^StreamsConfig configuration ^Topology topology]
  (ProcessorTopologyTestDriver. configuration topology))

(defn process
  ([^ProcessorTopologyTestDriver driver topic-name k v]
   (.process driver topic-name k v))
  ([^ProcessorTopologyTestDriver driver topic-name k v k-ser v-ser]
   (.process driver topic-name k v k-ser v-ser)))

(defn read-output
  ([^ProcessorTopologyTestDriver driver topic-name]
   (.readOutput driver topic-name))
  ([^ProcessorTopologyTestDriver driver topic-name k-deser v-deser]
   (.readOutput driver topic-name k-deser v-deser)))