(ns kastr.core
  (:require [schema.core :as schema]
            [taoensso.timbre :as log])
  (:import (org.apache.kafka.streams StreamsConfig StreamsBuilder KafkaStreams Topology KeyValue Consumed Topology$AutoOffsetReset)
           (org.apache.kafka.common.serialization Serdes Serde)
           (org.apache.kafka.clients.consumer ConsumerConfig)
           (org.apache.kafka.streams.kstream KStream ValueMapper KeyValueMapper Produced)
           (java.util Properties Collection)
           (org.apache.kafka.streams.processor TimestampExtractor)))

(schema/defschema KafkaStreamsConfigurationSchema
  {:application-id schema/Str
   :bootstrap-servers schema/Str
   (schema/optional-key :num-stream-threads) schema/Num
   (schema/optional-key :auto-offset-reset) (schema/enum :earliest :latest)
   (schema/optional-key :key-serde-class) schema/Str
   (schema/optional-key :value-serde-class) schema/Str})

(def configuration-mappings
  {:application-id StreamsConfig/APPLICATION_ID_CONFIG
   :bootstrap-servers StreamsConfig/BOOTSTRAP_SERVERS_CONFIG
   :num-stream-threads StreamsConfig/NUM_STREAM_THREADS_CONFIG
   :key-serde-class StreamsConfig/KEY_SERDE_CLASS_CONFIG
   :value-serde-class StreamsConfig/VALUE_SERDE_CLASS_CONFIG
   :auto-offset-reset ConsumerConfig/AUTO_OFFSET_RESET_CONFIG})

(defn configuration-mapping-value
  [k v]
  "Cast configuration values"
  (case k
    :auto-offset-reset (name v)
    :num-stream-threads (str v)
    v))

(defn streams-config
  [configuration]
  (StreamsConfig. configuration))

(schema/defn ^:always-validate kafka-streams-configuration
  "Create a Properties object based on the configuration"
  [configuration :- KafkaStreamsConfigurationSchema]
  (let [properties (Properties.)]
    (doseq [[k v] configuration]
      (if-let [m (k configuration-mappings)]
        (.put properties m (configuration-mapping-value k v))
        (log/warn "Unknown Kafka Streams configuration option: " {k v})))
    properties))

(defn streams-builder
  []
  (StreamsBuilder.))

(defn build
  [^StreamsBuilder builder]
  (.build builder))

(defn init
  "Initialise the stream instance"
  [^Topology topology ^StreamsConfig configuration]
  (KafkaStreams. topology configuration))

(defn clean
  "Delete all stream instance data in local state store"
  [^KafkaStreams stream]
  (.cleanUp stream))

(defn start
  "Start the stream instance"
  [^KafkaStreams stream]
  (.start stream))

(defn stop
  "Shutdown the stream instance "
  [^KafkaStreams stream]
  (.close stream))

(defn consumed
  "Create a Consumed instance for the specified serde, timestamp extractor, and auto offset reset"
  ([^Serde k-serde ^Serde v-serde ^TimestampExtractor timestamp-extractor ^Topology$AutoOffsetReset auto-offset-reset]
   (Consumed/with k-serde v-serde timestamp-extractor auto-offset-reset)))

(defn produced
  "Create a Produced instance for the specified serde"
  ([^Serde k-serde ^Serde v-serde]
   (Produced/with k-serde v-serde)))

(defn ^KStream stream
  "Create a KStream instance from the specified topics"
  ([^StreamsBuilder builder ^Collection input-topics ^Serde k-serde ^Serde v-serde ^TimestampExtractor timestamp-extractor ^Topology$AutoOffsetReset auto-offset-reset]
   (.stream builder input-topics (consumed k-serde v-serde timestamp-extractor auto-offset-reset))))

(defn to
  "Materialize the stream to a topic"
  ([^KStream stream ^String topic-name]
   (.to stream topic-name))
  ([^KStream stream ^String topic-name ^Serde k-serde ^Serde v-serde]
   (.to stream topic-name (produced k-serde v-serde))))

(defn- key-value-mapper [f]
  (reify KeyValueMapper
    (apply [_ k v] (f k v))))

(defn- value-mapper [f]
  (reify ValueMapper
    (apply [_ v] (f v))))

(defn map-kv
  [^KStream stream f]
  (->> (key-value-mapper
         (fn [k v]
           (let [[k v] (f k v)]
             (KeyValue. k v))))
       (.map stream)))

(defn map-v
  [^KStream stream f]
  (->> (value-mapper f)
       (.mapValues stream)))

(defn flat-map-kv
  [^KStream stream f]
  (->> (key-value-mapper
         (fn [k v]
           (map
             (fn [e]
               (let [[k v] e]
                 (KeyValue. k v)))
             (f k v))))
       (.flatMap stream)))

(defn flat-map-v
  [^KStream s f]
  (.flatMapValues s (value-mapper f)))

(def serde-string (Serdes/String))
(def serde-long (Serdes/Long))
(def serde-short (Serdes/Short))
(def serde-integer (Serdes/Integer))
(def serde-float (Serdes/Float))
(def serde-double (Serdes/Double))

(defn get-serializer
  "Get serializer from Serde"
  [^Serde serde]
  (.serializer serde))

(defn get-deserializer
  "Get deserializer from Serde"
  [^Serde serde]
  (.deserializer serde))