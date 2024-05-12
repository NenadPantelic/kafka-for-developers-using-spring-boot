## Traditional Messaging system vs Kafka Streaming Platform

Traditional Messaging system:

1. Transient Message Persistence
2. Broker's responsibility to keep track of consumed messages
3. Target a specific Consumer
4. Not a distributed system

Kafka Streaming Platform

1. Stores events based on a retention time. Events are immutable.
2. Consumers Responsibility to keep track of consumed messages
3. Any consumer can access a message from the broker
4. It's a distributed streaming platform

## Kafka terminologies and client APIs

- Kafka has a clustered deployment, it's composed of brokers
- To manage multiple brokers, we use a distributed manager ZooKeeper
- Kafka producer: a component that produces the data that is ingested into Kafka
- Kafka consumer: a component that consumes the data stored in Kafka
- Kafka APIs:
    - Kafka Connect API - it has two different types of connectors (Source Connector and Sink Connector)
        - Source Connector pulls the data from external source (the database, File System, Elasticsearch...)
        - Sink Connector pulls the data from Kafka to an external target
    - Producer and Consumer APIs
    - Stream API - takes the data a creates a stream where it can apply many transformations and put it back to Kafka

## Kafka internals

#### Kafka topics

- Topic is an entity in Kafka with a name:
    - producer creates a message for topic A
    - Kafka broker stores that message in topic A
    - consumer polls topic A and fetches messages of that topic

#### Kafka partitions

- Partition is where the message lives inside the topic
- each topic is created with one or more partitions

![](images/kafka_partitions_topics.png)

- each partition is ordered, immutable sequence of records
- each record is assigned a sequential number called **offset**
- each partition is independent of each other
- ordering is guaranteed only at the partition level
- partition continuously grows as new records are produced
- all the records are persisted in a commit log in the file system where Kafka is installed

If a topic has multiple partitions, there is a component called partitioner that should delegate an incoming message to
a proper partition. Kafka produce API supports sending a key (optional) and a value (mandatory). If the key is passed,
then it will be used as a distribution key to choose the partition. Otherwise, partitioner applies the Round-Robin
distribution approach. With the approach of RR distribution without key, there is no guarantee that the consumer will
read messages in the order they arrived to topic, since the consumer reads messages from all partitions at the same
time.

If the key is present, then the partitioner calculates a hash, so all messages with the same key goes to the same
partition.

### Offsets

- Any message produced in Kafka has a unique ID called offset.
- Consumer have three options:
    - from-beginning (read messages from the very beginning)
    - latest (read messages that arrive only after the consumer is brought up)
    - specific offset (only programmatically)
- Consumer offsets are stored in an internal topic `__consumer_offsets` -> when the consumer consumes messages, it moves
  offset one by one. If the consumer crashes, once it gets back, it will read the latest consumer offset value from an
  internal topic `__consumer_offsets` to know where to start from.
  (check consumer offsets internal topic with `docker exec --interactive --tty kafka1  \
  kafka-topics --bootstrap-server kafka1:19092 --list`)

### Consumer groups:

- group.id is mandatory; it plays a major role when it comes to scalable message consumption.
  ![](images/kafka_consumer_groups.png)
  If the consumer belong to the same consumer group, they will consume messages stored in partitions in a way that
  consumers
  will listen to those partitions, so that every partition is being consumed by one consumer and the number of
  partitions
  is (almost) evenly distributed amongst consumers. Consumer poll is single-threaded.
  Group id should be unique across applications
- Consumer groups are coordinated by the broker

### Retention policy:

- commit log is backed by the file system (e.g. `log.dirs=/tmp/kafka-logs`); when the message is committed to the
  file system, then the message is available in commit log
- retention policy is configured with `log.retention.hours` in `/etc/kafka/server.properties` file (the default value is
  168 hrs
  = 7 days)
- the data is present in `/var/lib/kafka/data` -> you can find your topics there

### Distributed system

- Kafka is a distributed system providing:
    - availability and fault tolerance
    - reliable work distribution
    - easily scalable
    - handling concurrency is fairly easy
- Kafka operates as a cluster (leader and followers) managed by ZK (heartbeat system, Kafka brokers sending beats at
  some time interval)
    - load balanced traffic
    - easy to scale by adding more brokers based on the need
- Kafka uses replication to ensure the redundancy and bottlenecks
- How Kafka distributes client requests across brokers:
    1. Creating a topic

    - one of them will be the controller (usually the first broker that has joined the cluster)
    - when the create topic command is shut, ZK delegates it to the controller

    2. Producing a message

    - controller distributes the ownership of partitions across brokers (e.g. Partition 0 sitting in Broker 1, Partition
      1 sitting in Broker 2, etc.)
    - now, brokers are leaders of their respective partition
    - partitioner: a component that determines where the message should be stored (in which partition)
    - when the message is sent and the partitioner decided where to store it, the message is sent to the leader of that
      particular partition, and it is persisted in its file system

    3. Consuming a message
       Kafka consuming loop is listening to all partitions and fetches messages from all of them (only targets the
       partition
       leader)
    4. Consumer groups
       Let's say we have multiple consumers with the same group ID composing a single Consumer Group. Now, partitioner
       assigns
       a topic to a particular consumer which then consumes messages from the leader of that partition.

### How Kafka handles data loss?

- Let's say a leader of some partition goes down. How Kafka handles that loss, since the data available at that broker
  is now lost?
- It uses __Replication__. Let's say we have defined the replication factor = 3, once the message is copied/replicated
  to other brokers (2 of them) and stored in their file system (follower replicas now have the same message).
- Now, if one leader fails (let's say the leader of partition 0), ZK will get notified and will promote one of the
  existing brokers to the leader of that partition.

  ```sh
    docker exec --interactive --tty kafka1  \
    kafka-topics --bootstrap-server kafka1:19092 \
    --create \
    --topic test-topic \
    --replication-factor 3 --partitions 3
  ```

#### In-Sync replicas (ISR)

- Represents the number of replicas in sync with each other in the cluster (includes both leader and follower replicas)
- Recommended value is always greater than 1 (ideal value is __ISR == Replication factor__)
- This can be controlled by __min.insync.replicas__ property (it can be set at the broker or topic level)

### Kafka template

- produce records and send them to Kafka topic
- similar to JDBCTemplate for DB
- What happens when `KafkaTemplate.send()` is triggered?

1. The first step is serialization, since Kafka operate with byte values
    - there are two types of serializers (`key.serializer` and `value.serializer`)
2. The next step is partitioning keys/, messages and sending them a proper partition; the `DefaultPartitioner` component
   usually does the work just fine
3. `RecordAccumulator` - we do not want to actually send to Kafka message-by-message. We have an accumulator set in
   place
   to reduce the number of Kafka calls and by that prevent an overloading
    - it has `RecordBatch` slots (for every partition we have) that has a capacity of `batch.size` bytes and once the
      threshold is reached, it flushes the data to Kafka. There is one more property - `buffer.memory` which is the
      accumulative capacity of record batches. If the batch size threshold is never/rarely reached, Kafka has another
      property to control flushing - `linger.ms` = it sends the data to Kafka every `linger.ms` milliseconds (sooner if
      the
      threshold is passed).

- Configuring KafkaTemplate:
    - mandatory values:
        - `bootstrap-servers: localhost:9092, localhost:9093, localhost:9094`
        - `key-serializer: org.apache.kafka.common.serialization.IntegerSerializer`
        - `value-serializer: org.apache.kafka.common.serialization.StringSerializer`
    - via `application.yml` file:
  ```yaml
  spring:
    profile: local
    kafka:
      producer:
         bootstrap-servers: localhost:9092, localhost:9093, localhost:9094
         key-serializer: org.apache.kafka.common.serialization.IntegerSerializer
         value-serializer: org.apache.kafka.common.serialization.StringSerializer       
  ```

### How autoconfiguration works?

- `spring-boot-autoconfigure -> META-INF > spring.factories`
- The target properties are:
  ```  
  org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
  # this means that if this class is found in the classpath, the above configuration will be activate
  org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration.ConditionalOnClass=org.springframework.kafka.core.KafkaTemplate
  ```

### Kafka producer configurations

- acks (0, 1 and -1)
    - acks = 1 -> guarantees message is written to a leader
    - acks = -1 -> guarantees message is written to a leader and to all the replicas (default)
    - acks = 0 -> no guarantee (not recommended)

- retries (integer value = [0 - 2147483647])
    - the default value = 2147483647

- `retry.backoff.ms`
    - integer value represented in milliseconds
    - the default value is 100ms

### Kafka consumer

- `MessageListenerContainer`
    - `KafkaMessageListenerContainer`
        - polls the records
        - commits the offsets
        - single-threaded
    - `ConcurrentMessageListenerContainer`
        - represents multiple `KafkaMessageListenerContainer`
- `@KafkaListener` annotation (uses `ConcurrentMessageListenerContainer` behind the scene)
  - the easiest way to build KafkaConsumer
  

- Rebalance
  - changing the partition ownership from one consumer to another
  - there is a component called group coordinator which when the new consumer instance with the same group.id is brought
  up, will rebalance the ownership of the partitions, so it evenly distribute them between the consumer instances
  - and vice-versa, if a consumer is brought down, assignor will again rebalance the partitions

- Offsets
  - when the consumer polls and reads the records from Kafka, at the end it commits the offset to `__consumer_offsets` 
  topic
  - there are various committing offsets strategies; take into account that `poll()` actually reads multiple records at 
  once