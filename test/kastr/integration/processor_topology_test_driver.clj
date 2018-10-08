(ns kastr.integration.processor-topology-test-driver
  (:import (org.apache.kafka.test ProcessorTopologyTestDriver)
           (org.apache.kafka.streams StreamsConfig Topology)
           (org.apache.kafka.common.serialization Serializer Deserializer)))

(defn processor-topology-test-driver
  [^StreamsConfig configuration ^Topology topology]
  (ProcessorTopologyTestDriver. configuration topology))

(defn process
  ([^ProcessorTopologyTestDriver driver ^String topic-name k v]
   (.process driver topic-name k v))
  ([^ProcessorTopologyTestDriver driver ^String topic-name k v ^Serializer k-ser ^Serializer v-ser]
   (.process driver topic-name k v k-ser v-ser))
  ([^ProcessorTopologyTestDriver driver ^String topic-name k v ^Serializer k-ser ^Serializer v-ser ^Long timestamp]
   (.process driver topic-name k v k-ser v-ser timestamp)))

(defn read-output
  ([^ProcessorTopologyTestDriver driver ^String topic-name]
   (.readOutput driver topic-name))
  ([^ProcessorTopologyTestDriver driver ^String topic-name ^Deserializer k-deser ^Deserializer v-deser]
   (.readOutput driver topic-name k-deser v-deser)))