# kastr
Clojure wrapper for Kafka Streams

[![Build Status](https://travis-ci.org/ThatGuyHughesy/kastr.svg?branch=master)](https://travis-ci.org/ThatGuyHughesy/kastr)
[![Clojars Project](https://img.shields.io/clojars/v/kastr.svg)](https://clojars.org/kastr)

## Installation

Declare kastr in your project.clj:

[![Clojars Project](http://clojars.org/kastr/latest-version.svg)](http://clojars.org/kastr)
  
Use kastr in your clojure code:

```clojure
(require '[kastr.core :as kastr])
```

## Configuration

#### Required

:application-id - Name of Kafka Streams application

:bootstrap-servers - Comma separated list of Kafka brokers

:zookeeper-connect - Comma separated list of Zookeeper servers


#### Optional
:num-stream-threads (Default: 1) - Number of threads the stream instance will run

:key-serde-class (Default: String) - Default key serializer/deserializer

:value-serde-class (Default: String) - Default value serializer/deserializer

:auto-offset-reset (Default: :earliest) - Kafka topic offset the stream instance will read from

Example:

```clojure
{:kafka-streams {:application-id "threat-detection-1"
                 :bootstrap-servers "localhost:9092"
                 :zookeeper-connect "localhost:2181"
                 :num-stream-threads 8}
 :job {:input-topic "input-messages"
       :output-topic "output-messages"}}
```

## Running

I would recommend using [Component](https://github.com/stuartsierra/component) for managing the Kafka Streams runtime.

Example:

```clojure
(defrecord KafkaStreams
  [configuration kafka-streams-topology kafka-streams]
  component/Lifecycle
  (start [component]
    (info "Starting Kafka Streams")
    (let [configuration (kafka-streams-configuration configuration)
          kafka-streams (init configuration kafka-streams-topology)]
      (clean kafka-streams)
      (start kafka-streams)
      (assoc component :kafka-streams kafka-streams)))
  (stop [component]
    (try
      (finally
        (info "Shutting down Kafka Streams")
        (if kafka-streams
          (stop kafka-streams))))
    (assoc component
      :kafka-streams nil)))
```

The `kafka-streams-topology` is where you carry out your logic.

Basic Example:

Take messages from the input topic and send them to the output topic.

```clojure
(defn basic-kafka-streams-topology
  [configuration stream-builder]
  (let [{:keys [job]} configuration
        {:keys [input-topic output-topic]} job
        message-stream (stream stream-builder [input-topic])]
    (to message-stream output-topic)))
```

## Documentation

I'm currently working on this so for now [here](https://docs.confluent.io/current/streams/developer-guide.html) is the official Developer Guide.

## Development

### Testing

Run tests

```sh
$ lein test
```

## Contributing

Want to become a Kastr [contributor](https://github.com/ThatGuyHughesy/kastr/blob/master/CONTRIBUTORS.md)?  
Then checkout our [code of conduct](https://github.com/ThatGuyHughesy/kastr/blob/master/CODE_OF_CONDUCT.md) and [contributing guidelines](https://github.com/ThatGuyHughesy/kastr/blob/master/CONTRIBUTING.md).

## Copyright & License

Copyright (c) 2017 Conor Hughes - Released under the MIT license.
