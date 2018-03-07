(ns kastr.core
  (:require [clojure.data.json :as json]
            [schema.core :as schema]
            [taoensso.timbre :as log])
  (:import (org.apache.kafka.streams StreamsConfig StreamsBuilder KafkaStreams Topology)
           (org.apache.kafka.common.serialization Serdes Serdes$StringSerde Serde)
           (org.apache.kafka.clients.consumer ConsumerConfig)
           (org.apache.kafka.streams.kstream KStream KStreamBuilder)
           (java.util Properties Collection)))

(schema/defschema KafkaStreamsConfigurationSchema
  {:application-id schema/Str
   :bootstrap-servers schema/Str
   :zookeeper-connect schema/Str
   (schema/optional-key :num-stream-threads) schema/Num
   (schema/optional-key :key-serde-class) schema/Str
   (schema/optional-key :value-serde-class) schema/Str
   (schema/optional-key :auto-offset-reset) (schema/enum :earliest :latest)})

(def configuration-mappings
  {:application-id StreamsConfig/APPLICATION_ID_CONFIG
   :bootstrap-servers StreamsConfig/BOOTSTRAP_SERVERS_CONFIG
   :zookeeper-connect StreamsConfig/ZOOKEEPER_CONNECT_CONFIG
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
  (let [configuration (merge {:num-stream-threads 1
                              :key-serde-class (-> Serdes$StringSerde .getName)
                              :value-serde-class (-> Serdes$StringSerde .getName)
                              :auto-offset-reset :earliest}
                             configuration)
        properties (Properties.)]
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

(defn ^KStream stream
  "Create a KStream instance from the specified topics"
  [^StreamsBuilder builder input-topics]
  (.stream builder input-topics))

(defn to
  "Materialize the stream to a topic"
  ([^KStream stream topic-name]
   (.to stream topic-name)))

(def serde-string (Serdes/String))
(def serde-long (Serdes/Long))

(defn get-serializer
  "Get serializer from Serde"
  [^Serde serde]
  (.serializer serde))

(defn get-deserializer
  "Get deserializer from Serde"
  [^Serde serde]
  (.deserializer serde))