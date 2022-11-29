# kafka架构概述

在消息拉取和发送的过程中，可以分为 produce,customer,broker 三个角色。

<img src=".\img\kafak_架构概述.png" title="" alt="" data-align="inline">

## 名词解释

- topic:

Kafka对消息进行归类，发送到集群的每一条消息都要指定一个topic

- Partition:

物理上的概念，每个topic包含一个或多个partition，一个partition对应一个文件夹，这个文件夹下存储partition的数据和索引文件，每个partition内部是有序的

![](./img/partition_storage.png)

replicas：对每个partition而言，或有多个副本，副本数量取决于topic的复制因子，kafka会尽量把副本放到不同的broker上。对每个partition的副本，会由leader和follower两种角色，消息发送时，producer只会发送给leader。消息拉取时，若不指定优先副本或优先副本已挂，也会从leader拉取。

![](./img/kafka_repilce.png)

customeGroup：

consumer group是kafka提供的可扩展且具有容错性的消费者机制。组内可以有多个消费者或消费者实例(consumer instance)，它们共享一个公共的ID，即group ID。组内的所有消费者协调在一起来消费订阅主题(subscribed topics)的所有分区(partition)。每个分区只能由同一个消费组内的一个consumer来消费。

- 一个consumer group下可以有一个或多个consumer instance，consumer instance可以是一个进程，也可以是一个线程
- group.id是一个字符串，唯一标识一个consumer group
- consumer group下订阅的topic下的每个分区只能分配给某个group下的一个consumer(当然该分区还可以被分配给其他group)

## Kafka 如何保证高可靠

1. 数据可靠性
   
   Kafka 的**分区多副本架构**是 Kafka 可靠性保证的核心，把消息写入多个副本可以使Kafka 在发生崩溃时仍能保证消息的持久性。
   
   - Producter往Broker发送消息基于 acks 的消息确认机制
     
     - acks = 0
       
       意味着如果生产者能够通过网络把消息发送出去，那么就认为消息已成功写入Kafka。在这种情况下还是有可能发生错误，比如发送的对象未能被序列化或者网卡发生故障。
     
     - acks = 1
       
       意味若 Leader 在收到消息并把它写入到分区数据文件（不一定同步到磁盘上）时会返回确认或错误响应。在这个模式下，如果发生正常的 Leader 选举，生产者会在选举时收到一个 LeaderNotAvailableException 异常，如果生产者能恰当地处理这个错误，它会重试发送悄息，最终消息会安全到达新的 Leader 那里。不过在这个模式下仍然有可能丢失数据，比如消息已经成功写入 Leader，但在消息被复制到 follower 副本之前 Leader发生崩溃。
     
     - acks = all（这个和 request.required.acks = -1 含义一样）
       
       意味着 Leader 在返回确认或错误响应之前，会等待所有同步副本都收到悄息。如果和 min.insync.replicas 参数结合起来，就可以决定在返回确认前至少有多少个副本能够收到悄息，生产者会一直重试直到消息被成功提交。不过这也是最慢的做法，因为生产者在继续发送其他消息之前需要等待所有副本都收到当前的消息。
   
   - 分区多副本机制
     . leader选举机制

2. 数据一致性
   
   ![](./img/kafka_partition_message.png)
   
   假设分区的副本为5，其中副本5是 Leader，其他副本是 follower，并且在 ISR 列表里面。虽然副本5已经写入了 Message4，但是 Consumer 只能读取到 Message2。因为所有的 ISR 都同步了 Message2，只有 High Water Mark 以上的消息才支持 Consumer 读取，而 High Water Mark 取决于 ISR 列表里面偏移量最小的分区。
   这样做的原因是还没有被足够多副本复制的消息被认为是不安全的，如果 Leader 发生崩溃，另一个副本成为新 Leader，那么这些消息很可能丢失了。如果我们允许消费者读取这些消息，可能就会破坏一致性。试想，一个消费者从当前 Leader（副本0） 读取并处理了 Message4，这个时候 Leader 挂掉了，选举了副本1为新的 Leader，这时候另一个消费者再去从新的 Leader 读取消息，发现这个消息其实并不存在，这就导致了数据不一致性问题。
   当然，引入了 High Water Mark 机制，会导致 Broker 间的消息复制因为某些原因变慢，那么消息到达消费者的时间也会随之变长（因为我们会先等待消息复制完毕）。延迟时间可以通过参数 replica.lag.time.max.ms 参数配置，它指定了副本在复制消息时可被允许的最大延迟时间。

## kafka效率保证

1. 顺序读写

2. 零拷贝

3. 文件分段

4. 批量发送

5. 数据压缩

# kakfa协议概述

[Apache Kafka 协议参考](https://kafka.apache.org/protocol#protocol_preliminaries)

### 网络

kafka 基于tcp协议。由客户端发起socket连接，然后安顺序发起请求并接受对应的响应。

客户端需要维护到多个broker的连接。但是通常来说，单个客户端对单个broker不需要连接池，只需要维护一个连接。

服务端保证了对单个tcp连接，**请求会按照送达顺序被处理，并按照同样的顺序返回**。(由Selector 和 Processor保证，同时影响了客户端底层设计)

### 分区和启动

kafka是一个分区系统，不是说有的srevers都有完整的数据，数据会被分散在不同的分区中，每个分区都会有各自的复制。

消息发送到哪个分区中由client决定，broker不会强制指定哪些消息应该被发送到特定的分区中。

拉取或者发送消息的请求必须发送给当前分区的leader，leader选举由broker完成。

所有的kafka broker都可以返回 主题是否存在，主题有哪些分区，哪个地址可以被直接连接来发送请求的 元信息，

client 不需要通过平凡的轮询来查询集群的变化，可以拉取一次元数据然后缓存它，直到接收到元数据过期的错误。这个错误可以来着以下2钟情况：1，某个特定的broker 发生socket错误，2.在响应做特定的错误码标识（ps:实际上MetaData有个可配置的有效期时间，超过这个有效期时间后会去更新MetaData）

### 分区策略

kafka分区策略是客户端可以实现的一个部分。可以使用kafka提供的分区策略或在客户端自定义分区策略来实现消息均衡或将特定的消息发送到特定的分区中。

### 批量

kafka api 鼓励你把小东西批量放在一起来提高效率。

### 兼容性

kafka具有双向兼容性。旧客户端可以新broker 发起请求， 新客户端也可以向旧broker发起请求。在所有的请求被发送前，client会发送API KEY 和 API VERSION，都是16bit的number。

下图是协商支持的api版本流程

![](D:\work_s\java\kafka_adapter_gradle\doc\img\kafka_api_version_seq.png)

### SASL授权流程

todo

# 通用协议

#### API_VERSION

##### 功能

kafka兼容性实现支撑协议，查询服务端支持的协议版本，客户端会根据返回的值使用双方支持的最高协议版本构建请求。生产者和消费总是在一开始就发送该协议。

#### METADATA

##### 功能

查询主题对应的分区信息元数据，消费者和生产者均会查询。查询时机由没有对应主题的元数据或metadata过期时间共同控制 ， 对应参数：metadata.max.age.ms，实际客户端的代码中，会再NetworkClient的poll()方法中进行检查。

- 需要返回所有的broker节点信息

- 需要返回对应topic的分区信息

- 如果配置允许，创建不存在的topic

- 如果未指定查询的topic,需要返回所有的topic(eg. 消费者正则模式)

##### 相关错误码

```
Errors.LISTENER_NOT_FOUND
Errors.INVALID_TOPIC_EXCEPTION
Errors.TOPIC_AUTHORIZATION_FAILED
```

##### 协议详情

- 请求
  
  ```
  Metadata Request (Version: 12) => [topics] allow_auto_topic_creation include_topic_authorized_operations TAG_BUFFER 
  topics => topic_id name TAG_BUFFER 
  topic_id => UUID
   name => COMPACT_NULLABLE_STRING
   allow_auto_topic_creation => BOOLEAN
   include_topic_authorized_operations => BOOLEAN
  ```
  
  | FIELD                               | DESCRIPTION                                                                                                                                                        |
  | ----------------------------------- |:------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
  | topics                              | The topics to fetch metadata for.<br/>消费者以正则模式拉取时，topics为空数组，此时服务端需要返回所有的topic                                                                                     |
  | topic_id                            | The topic id.                                                                                                                                                      |
  | name                                | The topic name.                                                                                                                                                    |
  | _tagged_fields                      | The tagged fields                                                                                                                                                  |
  | allow_auto_topic_creation           | If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.<br/>客户端默认发送为true,是否创建取决于broker本身的配置 |
  | include_topic_authorized_operations | Whether to include topic authorized operations.kafka admin client<br/> 相关功能，客户端默认为false                                                                            |
  | _tagged_fields                      | The tagged fields                                                                                                                                                  |

- 响应
  
  ```
  Metadata Response (Version: 12) => throttle_time_ms [brokers] cluster_id controller_id [topics] TAG_BUFFER 
    throttle_time_ms => INT32
    brokers => node_id host port rack TAG_BUFFER 
      node_id => INT32
      host => COMPACT_STRING
      port => INT32
      rack => COMPACT_NULLABLE_STRING
    cluster_id => COMPACT_NULLABLE_STRING
    controller_id => INT32
    topics => error_code name topic_id is_internal [partitions] topic_authorized_operations TAG_BUFFER 
      error_code => INT16
      name => COMPACT_NULLABLE_STRING
      topic_id => UUID
      is_internal => BOOLEAN
      partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas] TAG_BUFFER 
        error_code => INT16
        partition_index => INT32
        leader_id => INT32
        leader_epoch => INT32
        replica_nodes => INT32
        isr_nodes => INT32
        offline_replicas => INT32
      topic_authorized_operations => INT32
  ```
  
  | FIELD                       | DESCRIPTION                                                                                                                                                                                                                                                                                                                                                            |
  | --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | throttle_time_ms            | The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.                                                                                                                                                                                                                           |
  | brokers                     | Each broker in the response.<br/>所有的brokers                                                                                                                                                                                                                                                                                                                            |
  | node_id                     | The broker ID.<br/>后续的所有协议均会以id 来寻找地址                                                                                                                                                                                                                                                                                                                                  |
  | host                        | The broker hostname.                                                                                                                                                                                                                                                                                                                                                   |
  | port                        | The broker port.                                                                                                                                                                                                                                                                                                                                                       |
  | rack                        | The rack of the broker, or null if it has not been assigned to a rack.null 即可                                                                                                                                                                                                                                                                                          |
  | _tagged_fields              | The tagged fields                                                                                                                                                                                                                                                                                                                                                      |
  | cluster_id                  | The cluster ID that responding broker belongs to.                                                                                                                                                                                                                                                                                                                      |
  | controller_id               | The ID of the controller broker.<br/>如果没有控制器 可以使用MetadataResponse.*NO_CONTROLLER_ID*（-1）[Kafka实战宝典：Kafka的控制器controller详解 - 腾讯云开发者社区-腾讯云 (tencent.com)](https://cloud.tencent.com/developer/article/1688442)主要用于admin client.<br/>对客户端的逻辑没有影响。                                                                                                                          |
  | topics                      | Each topic in the response.                                                                                                                                                                                                                                                                                                                                            |
  | error_code                  | The topic error, or 0 if there was no error.                                                                                                                                                                                                                                                                                                                           |
  | name                        | The topic name.                                                                                                                                                                                                                                                                                                                                                        |
  | topic_id                    | The topic id.                                                                                                                                                                                                                                                                                                                                                          |
  | is_internal                 | True if the topic is internal.<br/>kafka内部topic,一般为false。典型的内部topic有____custom_offset,这个topic是用来管理消费进度的。                                                                                                                                                                                                                                                               |
  | partitions                  | Each partition in the topic.                                                                                                                                                                                                                                                                                                                                           |
  | error_code                  | The partition error, or 0 if there was no error.                                                                                                                                                                                                                                                                                                                       |
  | partition_index             | The partition index.                                                                                                                                                                                                                                                                                                                                                   |
  | leader_id                   | The ID of the leader broker.                                                                                                                                                                                                                                                                                                                                           |
  | leader_epoch                | The leader epoch of this partition.参考文档：[KIP-101 - 更改复制协议以使用 Leader Epoch 而不是 High Watermark 进行截断 (qq.com)](https://mp.weixin.qq.com/s?__biz=MjM5NzgyODc1Mw==&mid=2647754415&idx=1&sn=3dd59403fb1391a41d3f024f17cba7e3&chksm=bef127518986ae47f8382c8a171591345bd84ef96fbff4523b373728b79d0f3c953fbd471b9f&token=647212035&lang=zh_CN#rd)用来同步水位控制消息生产过程中会提交这个字段,对我们可以忽略 |
  | replica_nodes               | The set of all nodes that host this partition.这个partition 全部的节点（leader and follower）                                                                                                                                                                                                                                                                                   |
  | isr_nodes                   | The set of nodes that are in sync with the leader for this partition.<br/>同步中的副本，给adminClinet 使用的                                                                                                                                                                                                                                                                      |
  | offline_replicas            | The set of offline replicas of this partition.已经离线的副本，给adminClinet 使用的                                                                                                                                                                                                                                                                                                 |
  | _tagged_fields              | The tagged fields                                                                                                                                                                                                                                                                                                                                                      |
  | topic_authorized_operations | 32-bit bitfield to represent authorized operations for this topic.//默认值 -2147483648，对客户端无影响，使用在kafkaAdminClinet中                                                                                                                                                                                                                                                       |
  | _tagged_fields              | The tagged fields                                                                                                                                                                                                                                                                                                                                                      |
  | _tagged_fields              | The tagged fields                                                                                                                                                                                                                                                                                                                                                      |

# 生产者相关协议

#### PRODUCE

##### 功能

生产者发送消息

- kafka发送消息是会尽可能的以批量的模式发送

- 消息协议格式在kafka 0.10.0发生过一次变更，一下讨论以新版本为准

- 如果客户端选择了压缩算法kafka消息再发送中已经被压缩过

##### 请求

```
Produce Request (Version: 9) => transactional_id acks timeout_ms [topic_data] TAG_BUFFER 
  transactional_id => COMPACT_NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => name [partition_data] TAG_BUFFER 
    name => COMPACT_STRING
    partition_data => index records TAG_BUFFER 
      index => INT32
      records => COMPACT_RECORDS
```

| FIELD            | DESCRIPTION                                                                                                                                                                                                                                                                                 |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| transactional_id | The transactional ID, or null if the producer is not transactiona.<br/>暂不支持事务                                                                                                                                                                                                               |
| acks             | The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.<br/>副本同步参数acks 0不需要等待同步，等于0的时候，不需要broker返回responseacks 1仅leader //RAFT-1 全部 |
| timeout_ms       | The timeout to await a response in milliseconds.                                                                                                                                                                                                                                            |
| topic_data       | Each topic to produce to.                                                                                                                                                                                                                                                                   |
| name             | The topic name.                                                                                                                                                                                                                                                                             |
| partition_data   | Each partition to produce to.                                                                                                                                                                                                                                                               |
| index            | The partition index.                                                                                                                                                                                                                                                                        |
| records          | The record data to be produced.                                                                                                                                                                                                                                                             |
| _tagged_fields   | The tagged fields                                                                                                                                                                                                                                                                           |
| _tagged_fields   | The tagged fields                                                                                                                                                                                                                                                                           |
| _tagged_fields   | The tagged fields                                                                                                                                                                                                                                                                           |

##### 消息结构

![](./img/kafak_batch_maeesage.png)

对应类：

[org.apache.kafka.common.record.DefaultRecordBatch](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java)

[org.apache.kafka.common.record.RecordBatch](https://github.com/apache/kafka/blob/3.0/clients/src/main/java/org/apache/kafka/common/record/RecordBatch.java)

[Apache Kafka messageformat](https://kafka.apache.org/documentation/#messageformat)

RecordBatch

| 字段                   | 类型       | 说明                                                                                  |
| -------------------- | -------- | ----------------------------------------------------------------------------------- |
| baseOffset           | int64    | 当前 RecordBatch起始位置。Record 中的 offset delta与该baseOffset相加才得到真正的offset值                |
| batchLength          | int32    | RecordBatch 总长 (byte lenght , 包括头和所有的record)                                        |
| partitionLeaderEpoch | int32    | 标记 partition 中 leader replica 的元信息                                                  |
| magic                | int8     | 版本的魔术值，V2版本魔术值为2                                                                    |
| crc                  | int32    | 校验码， 效验部分从开始到结束全部数据，但除了partitionLeaderEpoch值                                        |
| attributes           | int16    | 消息属性，0~2:表示压缩类型 第3位：时间戳类型 第4位：是否是事务型记录 5表示ControlRecord，这类记录总是单条出现，它只在broker内处理     |
| lastOffsetDelta      | int32    | RecordBatch 最后一个 Record 的相对位移，用于 broker 确认 RecordBatch 中 Records 的组装正确性             |
| firstTimestamp       | int64    | RecordBatch 第一条 Record 的时间戳                                                         |
| maxTimestamp         | int64    | RecordBatch 中最大的时间戳，一般情况下是最后一条Record的时间戳，用于 broker 判断 RecordBatch 中 Records 的组装是否正确 |
| producerId           | int64    | 生产者编号，用于支持幂等性（Exactly Once 语义）                                                      |
| producerEpoch        | int16    | 同producerEpoch，支持幂等性                                                                |
| baseSequence         | int32    | 同上，支持幂等性，也用于效验是否重复Record                                                            |
| records              | [Record] |                                                                                     |

Record

| 字段              | 类型       | 说明                     |
| --------------- | -------- | ---------------------- |
| length          | varints  | 消息中长度                  |
| attributes      | int8     | unused，没有使用了，但仍占据了1B大小 |
| timestamp delta | varlong  | 时间戳增量。一般占据8个字节         |
| offset delta    | varint   | 位移增量                   |
| key length      | varint   | key的长度                 |
| key             | byte[]   | key的值                  |
| valuelen        | varint   | value值的长度              |
| value           | byte[]   | value的实际值              |
| headers         | [header] | 头部结构，支持应用级别的扩展         |

Header

| 字段                | 类型      | 说明       |
| ----------------- | ------- | -------- |
| headerKeyLength   | varints | 头部key的长度 |
| headerKey         | string  | 头部 key值  |
| headerValueLength | varint  | 头部值的长度   |
| value             | byte[]  | header的值 |

##### 响应

```
Produce Response (Version: 9) => [responses] throttle_time_ms TAG_BUFFER 
  responses => name [partition_responses] TAG_BUFFER 
    name => COMPACT_STRING
    partition_responses => index error_code base_offset log_append_time_ms log_start_offset [record_errors] error_message TAG_BUFFER 
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
      log_start_offset => INT64
      record_errors => batch_index batch_index_error_message TAG_BUFFER 
        batch_index => INT32
        batch_index_error_message => COMPACT_NULLABLE_STRING
      error_message => COMPACT_NULLABLE_STRING
  throttle_time_ms => INT32
```

| FIELD                     | DESCRIPTION                                                                                                                                                                                                                                       |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| responses                 | Each produce response                                                                                                                                                                                                                             |
| name                      | The topic name                                                                                                                                                                                                                                    |
| partition_responses       | Each partition that we produced to within the topic.                                                                                                                                                                                              |
| index                     | The partition index.                                                                                                                                                                                                                              |
| error_code                | The error code, or 0 if there was no error.                                                                                                                                                                                                       |
| base_offset               | The base offset.<br/>（该批次的base_offset,客户端会根据base_offset 计算offset，并且在事务管理器种会记录该base_offset到TxnPartitionEntry.lastAckedOffset中）                                                                                                                     |
| log_append_time_ms        | The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1. If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended. |
| log_start_offset          | The log start offset<br/>.分段日志起始位置（客户端没有用到）                                                                                                                                                                                                       |
| record_errors             | The batch indices of records that caused the batch to be dropped<br/>发送错误的records                                                                                                                                                                 |
| batch_index               | The batch index of the record that cause the batch to be dropped                                                                                                                                                                                  |
| batch_index_error_message | The error message of the record that caused the batch to be dropped                                                                                                                                                                               |
| _tagged_fields            | The tagged fields                                                                                                                                                                                                                                 |
| error_message             | The global error message summarizing the common root cause of the records that caused the batch to be dropped                                                                                                                                     |
| _tagged_fields            | The tagged fields                                                                                                                                                                                                                                 |
| _tagged_fields            | The tagged fields                                                                                                                                                                                                                                 |
| throttle_time_ms          | The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.                                                                                                      |
| _tagged_fields            | The tagged fields                                                                                                                                                                                                                                 |

##### 相关错误码

```
Errors.MESSAGE_TOO_LARGE
Errors.DUPLICATE_SEQUENCE_NUMBER
Errors.TOPIC_AUTHORIZATION_FAILED
Errors.CLUSTER_AUTHORIZATION_FAILED
 //batch_index_error_message 可以自定义提示语
```

##### 消息压缩

kafka broker默认消息压缩配置 为produce,表示尊重客户端使用的压缩算法，在压缩算法一致时，kafka broker 会将客户端的发送的消息整个储存下来，消费者拉取后，由消费者负责解压。如果不一致，会broker会解压后重新进行压缩。

produce消息压缩发生于`tryAppend` 过程中.此时写入的消息所有属性均已经被压缩
RecordBatch 头部属性修改 发生于 Sender 阶段，具体见
`Sender.sendProducerData() ->accumulator.drain() ->drainBatchesForOneNode()->batch.close()->recordsBuilder.close()`

不发生重新压缩的情况下，kafka broker直接储存整个RecordBatch 的data 部分,消息解压一般为customer完成

#### InitProducerId

##### 功能

实现生产者幂等,或者开始事务时，会向broker 申请ALLOCATE_PRODUCER_IDS,

ENABLE_IDEMPOTENCE_CONFIG 默认是true的

##### 请求

```
InitProducerId Request (Version: 4) => transactional_id transaction_timeout_ms producer_id producer_epoch TAG_BUFFER 
transactional_id => COMPACT_NULLABLE_STRING
 transaction_timeout_ms => INT32
 producer_id => INT64
 producer_epoch => INT16
```

| FIELD                  | DESCRIPTION                                                                                                                                             |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| transactional_id       | The transactional id, or null if the producer is not transactional.                                                                                     |
| transaction_timeout_ms | The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.            |
| producer_id            | The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.                                        |
| producer_epoch         | The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match. |
| _tagged_fields         | The tagged fields                                                                                                                                       |

##### 响应

```
InitProducerId Response (Version: 4) => throttle_time_ms error_code producer_id producer_epoch TAG_BUFFER 
throttle_time_ms => INT32
 error_code => INT16
 producer_id => INT64
 producer_epoch => INT16
```

| FIELD            | DESCRIPTION                                                                                                                                  |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| throttle_time_ms | The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. |
| error_code       | The error code, or 0 if there was no error.                                                                                                  |
| producer_id      | The current producer id.                                                                                                                     |
| producer_epoch   | The current epoch associated with the producer id.                                                                                           |
| _tagged_fields   | The tagged fields                                                                                                                            |

#### 消息发送流程概述

![](./img/kafka_produce_seq_class.png)

![](./img/kafka_produce_seq.png)

- 用户消息发送线程
  
  - produce 发送消息
  
  - 调用可能存在的interceptor处理
  
  - 阻塞到对应topic的元数据获取成功（确定分片信息等）
    
    - 如果能获取到对应的元数据直接返回
    
    - 否则添加需要查询元数据标识并且阻塞到超时或获取元数据成功。
  
  - 根据分区策略计算分区
  
  - 根据分区和主题计算出消息存放的队列
    
    - 如果队列不存在则初始化
  
  - 将该条消息放入队列中
    
    - 如果队列中最后一个BatchRecord 未达到最大容量，将消息放入该BatchRecord
    
    - 否则将创建一个新的BatchRecord,然后添加该条消息。
    
    - 在消息加入BatchRecord的过程中，将会使用对应的压缩算法进行消息压缩。

- sender线程
  
  - sender.runOnce
  
  - 检查是否需要更新元数据，如果需要构建metadataRequst
  
  - 轮询检查出所有已就绪的broker节点
  
  - 根据节点Id从累加器中找出需要发往该节点的消息然后进行批量打包
    
    - 会以topicPartition分组
  
  - 构建请求
  
  - selector.poll，详见后续的Selector概述章节

# 消费者相关协议

## 消费者初始化&rebalance流程概述

kafka消费者在同一个组内，一个partition最多只能被一个消费者消费，来保证消息的顺序性消费，同时可以实现kafka的拉取消息offset由customer维护。为了保证一个partition最多只能被一个消费者消费，在有customer启动/宕机的时，会发生rebalance。

 ![](./img/customer_init.png)

- 查找或等到coordinator可用
  
  - 如果coordinator可用是，唤醒心跳线程，维护定时心跳发送
  
  - 心跳响应可以通过特定的错误码来告知customer进入重平衡流程

- 发起join_group
  
  - 消费者初始化或被心跳响应通知后，会发起join_group
  
  - coordinator等待一段时间后才会对join_group请求进行响应。coordinator会尽可能等待更多的customer发送jon_group请求，来避免频繁的发生rebalance。

- 根据join_group响应判断是否是customer group leader

- 如果是leader 计算分区分配结果

- 发送sync_group请求，如果是leader,需要携带分配结果。
  
  - coordinator会等待到leader 发送sync_group后进行响应。

- 根据sync_group响应，服从分区分配结果。

- 初始化offset,metadata等

- 进入正式的消息拉取流程

#### coordinator状态图

![](./img/coordinator1.png)

## FIND_COORDINATOR

#### 功能

消费者初始化时，会查询coordinator,这个协调者会负责组成员、心跳事件、offset、再均衡的管理。

> kafka使用了一个特殊主题__customer_offset 来管理offset，默认该主题有50个分片，会根据消费组Id的 hash 来确定commit_offset消息被储存于那个分片，这个分片leader所在的broker就会成为该消费组的coordinator 

#### 请求

```
FindCoordinator Request (Version: 4) => key_type [coordinator_keys] TAG_BUFFER 
  key_type => INT8
  coordinator_keys => COMPACT_STRING
```

| FIELD          | DESCRIPTION                                                                                                  |
| -------------- | ------------------------------------------------------------------------------------------------------------ |
| key            | The coordinator key.消费者时，即GROUPID                                                                            |
| key_type       | The coordinator key type. (Group, transaction, etc.)*GROUP*((byte) 0), *TRANSACTION*((byte) 1);消费者时，固定为group |
| _tagged_fields | The tagged fields                                                                                            |

#### 响应

```
FindCoordinator Response (Version: 4) => throttle_time_ms [coordinators] TAG_BUFFER 
  throttle_time_ms => INT32
  coordinators => key node_id host port error_code error_message TAG_BUFFER 
    key => COMPACT_STRING
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
```

| FIELD            | DESCRIPTION                                                                                                                                  |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| throttle_time_ms | The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. |
| coordinators     | Each coordinator result in the response<br/>对消费者而言，客户端会对返回的集合做判断，如果集合大小不是1，会抛出错误                                                             |
| key              | The coordinator key.                                                                                                                         |
| node_id          | The node id.                                                                                                                                 |
| host             | The host name.                                                                                                                               |
| port             | The port.                                                                                                                                    |
| error_code       | The error code, or 0 if there was no error.                                                                                                  |
| error_message    | The error message, or null if there was no error.                                                                                            |
| _tagged_fields   | The tagged fields                                                                                                                            |
| _tagged_fields   | The tagged fields                                                                                                                            |

## JOIN_GROUP

#### 功能

coordinator就绪后，初始化或被通知再均衡后，通过该协议进入再均衡流程。

#### 请求

```
JoinGroup Request (Version: 9) => group_id session_timeout_ms rebalance_timeout_ms member_id group_instance_id protocol_type [protocols] reason TAG_BUFFER 
  group_id => COMPACT_STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  protocol_type => COMPACT_STRING
  protocols => name metadata TAG_BUFFER 
    name => COMPACT_STRING
    metadata => COMPACT_BYTES
  reason => COMPACT_NULLABLE_STRING
```

| FIELD                | DESCRIPTION                                                                                                                                                                            |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| group_id             | The group identifier.                                                                                                                                                                  |
| session_timeout_ms   | The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.心跳控制                                                                        |
| rebalance_timeout_ms | The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.对消费者而言是配置中max.poll.interval.ms，即2次poll之间的间隔，超过这个间隔会被移除消费组，触发重新平衡。 |
| member_id            | The member id assigned by the group coordinator.第一次加入时可能为空                                                                                                                             |
| group_instance_id    | The unique identifier of the consumer instance provided by end user.第一次加入时可能为空                                                                                                         |
| protocol_type        | The unique name the for class of protocols implemented by the group we want to join.对消费者而言，是“consumer”                                                                                 |
| protocols            | The list of protocols that the member supports.//上传的是各个消费者支持的分区策略 和 订阅topics，ps:如果是正则模式订阅，上一步MetaData中会返回所有的主题，客户端会在FindCoordinator后，选择符合的主题                                           |
| name                 | The protocol name.//分区策略名称                                                                                                                                                             |
| metadata             | The protocol metadata.<br/>`[{topics:"",userData:""ownedPartitions:"" ;第一次通常为空groupInstanceId：“”值Optional.*empty*()}]`                                                                 |
| _tagged_fields       | The tagged fields                                                                                                                                                                      |
| reason               | The reason why the member (re-)joins the group.                                                                                                                                        |
| _tagged_fields       | The tagged fields                                                                                                                                                                      |

#### 响应

```
JoinGroup Response (Version: 9) => throttle_time_ms error_code generation_id protocol_type protocol_name leader skip_assignment member_id [members] TAG_BUFFER 
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_type => COMPACT_NULLABLE_STRING
  protocol_name => COMPACT_NULLABLE_STRING
  leader => COMPACT_STRING
  skip_assignment => BOOLEAN
  member_id => COMPACT_STRING
  members => member_id group_instance_id metadata TAG_BUFFER 
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    metadata => COMPACT_BYTES
```

| FIELD             | DESCRIPTION                                                                                                                                  |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| throttle_time_ms  | The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. |
| error_code        | The error code, or 0 if there was no error.                                                                                                  |
| generation_id     | The generation ID of the group.                                                                                                              |
| protocol_type     | The group protocol name.//cons                                                                                                               |
| protocol_name     | The group protocol selected by the coordinator.                                                                                              |
| leader            | The leader of the group.                                                                                                                     |
| skip_assignment   | True if the leader must skip running the assignment.//设置为true时，被选为leader的customer不会返回分配结果                                                    |
| member_id         | The member ID assigned by the group coordinator.                                                                                             |
| members           |                                                                                                                                              |
| member_id         | The group member ID.//consumer-test_1-1-3e48638d-ab17-4eef-b382-18e7ac2896f1                                                                 |
| group_instance_id | The unique identifier of the consumer instance provided by end user.                                                                         |
| metadata          | The group member metadata.//客户端处理中似乎没有用到该字段                                                                                                  |
| _tagged_fields    | The tagged fields                                                                                                                            |
| _tagged_fields    | The tagged fields                                                                                                                            |

## SYNC_GROUP

#### 功能

在收到JOIN_GROUP response 之后，由客户端发起，请求获取分配结果。如果是topic leader,会同时上传分配结果

> kafka允许客户端自定义s消费者分配策略，配置参数：partition.assignment.strategy
> 默认设置：
> 
> ```
> define(PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
>  ConfigDef.Type.LIST,
>  Arrays.asList(RangeAssignor.class, CooperativeStickyAssignor.class),
>  new ConfigDef.NonNullValidator(),
>  ConfigDef.Importance.MEDIUM,>  PARTITION_ASSIGNMENT_STRATEGY_DOC)
> ```

#### 请求

```
SyncGroup Request (Version: 5) => group_id generation_id member_id group_instance_id protocol_type protocol_name [assignments] TAG_BUFFER 
group_id => COMPACT_STRING
 generation_id => INT32
 member_id => COMPACT_STRING
 group_instance_id => COMPACT_NULLABLE_STRING
 protocol_type => COMPACT_NULLABLE_STRING
 protocol_name => COMPACT_NULLABLE_STRING
 assignments => member_id assignment TAG_BUFFER 
 member_id => COMPACT_STRING
 assignment => COMPACT_BYTES
```

| FIELD             | DESCRIPTION                                                                                                                                                                                                                                                                |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| group_id          | The unique group identifier.                                                                                                                                                                                                                                               |
| generation_id     | The generation of the group.//分配的Gruop id,服务端会做校验，由JOIN_GROUP返回                                                                                                                                                                                                            |
| member_id         | The member ID assigned by the group.                                                                                                                                                                                                                                       |
| group_instance_id | The unique identifier of the consumer instance provided by end user.                                                                                                                                                                                                       |
| protocol_type     | The group protocol type.                                                                                                                                                                                                                                                   |
| protocol_name     | The group protocol name.                                                                                                                                                                                                                                                   |
| assignments       | Each assignment.分配结果<br/>`[ "key": "consumer-test_1-1-1d9f0fcd-1240-4e92-8567-e35b268c55a7",      "value": {         "partitions": [            {               "topic": "xxx",               "partition": 0            }         ],         "userData": null      }   }]` |
| member_id         | The ID of the member to assign.                                                                                                                                                                                                                                            |
| assignment        | The member assignment.                                                                                                                                                                                                                                                     |
| _tagged_fields    | The tagged fields                                                                                                                                                                                                                                                          |
| _tagged_fields    | The tagged fields                                                                                                                                                                                                                                                          |

#### 响应

```
SyncGroup Response (Version: 5) => throttle_time_ms error_code protocol_type protocol_name assignment TAG_BUFFER 
  throttle_time_ms => INT32
  error_code => INT16
  protocol_type => COMPACT_NULLABLE_STRING
  protocol_name => COMPACT_NULLABLE_STRING
  assignment => COMPACT_BYTES
```

| FIELD            | DESCRIPTION                                                                                                                                  |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| throttle_time_ms | The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. |
| error_code       | The error code, or 0 if there was no error.                                                                                                  |
| protocol_type    | The group protocol type.                                                                                                                     |
| protocol_name    | The group protocol name.                                                                                                                     |
| assignment       | The member assignment. <br/>`{"partitions":[{"topic":"xxxx",partition:0}]}`                                                                  |
| _tagged_fields   | The tagged fields                                                                                                                            |

## OFFSET_FETCH

##### 功能

拉取已commited offset,消费者启动SYNC_GROUP之后会进行查询，或者主动调用committed()接口也会发起查询，OFFSET 由Coordinator管理

##### 请求

```
OffsetFetch Request (Version: 8) => [groups] require_stable TAG_BUFFER 
  groups => group_id [topics] TAG_BUFFER 
    group_id => COMPACT_STRING
    topics => name [partition_indexes] TAG_BUFFER 
      name => COMPACT_STRING
      partition_indexes => INT32
  require_stable => BOOLEAN
```

| FIELD             | DESCRIPTION                                                                                                 |
| ----------------- | ----------------------------------------------------------------------------------------------------------- |
| groups            | Each group we would like to fetch offsets for                                                               |
| group_id          | The group ID.                                                                                               |
| topics            | Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.                     |
| name              | The topic name.                                                                                             |
| partition_indexes | The partition indexes we would like to fetch offsets for.                                                   |
| _tagged_fields    | The tagged fields                                                                                           |
| _tagged_fields    | The tagged fields                                                                                           |
| require_stable    | Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions. |
| _tagged_fields    | The tagged fields                                                                                           |

##### 响应

```
OffsetFetch Response (Version: 6) => throttle_time_ms [topics] error_code TAG_BUFFER 
  throttle_time_ms => INT32
  topics => name [partitions] TAG_BUFFER 
    name => COMPACT_STRING
    partitions => partition_index committed_offset committed_leader_epoch metadata error_code TAG_BUFFER 
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      metadata => COMPACT_NULLABLE_STRING
      error_code => INT16
  error_code => INT16
```

| FIELD                  | DESCRIPTION                                                                                                                                  |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| throttle_time_ms       | The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. |
| topics                 | The responses per topic.                                                                                                                     |
| name                   | The topic name.                                                                                                                              |
| partitions             | The responses per partition                                                                                                                  |
| partition_index        | The partition index.                                                                                                                         |
| committed_offset       | The committed message offset.                                                                                                                |
| committed_leader_epoch | The leader epoch.                                                                                                                            |
| metadata               | The partition metadata.                                                                                                                      |
| error_code             | The error code, or 0 if there was no error.                                                                                                  |
| error_code             | The top-level error code, or 0 if there was no error.                                                                                        |

##### 相关错误码

```
Errors.COORDINATOR_LOAD_IN_PROGRESS
Errors.NOT_COORDINATOR
Errors.GROUP_AUTHORIZATION_FAILED

//topic partition错误
Errors.UNKNOWN_TOPIC_OR_PARTITION
Errors.TOPIC_AUTHORIZATION_FAILED
Errors.UNSTABLE_OFFSET_COMMIT
```

## HEARTBEAT

##### 功能

向 Coordinator 发送心跳，超过 sessionTimeout 后未发心跳会被认为已离线，同时coordinator通过对该协议的响应来通知客户端进行再均衡

##### 请求

```
Heartbeat Request (Version: 4) => group_id generation_id member_id group_instance_id TAG_BUFFER 
  group_id => COMPACT_STRING
  generation_id => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
```

| FIELD             | DESCRIPTION                                                          |
| ----------------- | -------------------------------------------------------------------- |
| group_id          | The group id.//String                                                |
| generation_id     | The generation of the group.//                                       |
| member_id         | The member ID.                                                       |
| group_instance_id | The unique identifier of the consumer instance provided by end user. |
| _tagged_fields    | The tagged fields                                                    |

##### 响应

```
Heartbeat Response (Version: 4) => throttle_time_ms error_code TAG_BUFFER 
  throttle_time_ms => INT32
  error_code => INT16
```

| FIELD            | DESCRIPTION                                                                                                                                  |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| throttle_time_ms | The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. |
| error_code       | The error code, or 0 if there was no error.<br/>                                                                                             |
| _tagged_fields   | The tagged fields                                                                                                                            |

##### 相关错误码

```
Errors.COORDINATOR_NOT_AVAILABLE
Errors.NOT_COORDINATOR

//重平衡触发错误
Errors.REBALANCE_IN_PROGRESS

Errors.ILLEGAL_GENERATION
Errors.UNKNOWN_MEMBER_ID
Errors.FENCED_INSTANCE_ID
Errors.GROUP_AUTHORIZATION_FAILED
```

## OFFSET_COMMIT

##### 功能

提交offset commit, commited_offset 会作为customer重启或再均衡后拉取的起始位置

##### 请求

```
OffsetCommit Request (Version: 8) => group_id generation_id member_id group_instance_id [topics] TAG_BUFFER 
  group_id => COMPACT_STRING
  generation_id => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  topics => name [partitions] TAG_BUFFER 
    name => COMPACT_STRING
    partitions => partition_index committed_offset committed_leader_epoch committed_metadata TAG_BUFFER 
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => COMPACT_NULLABLE_STRING
```

| FIELD                  | DESCRIPTION                                                          |
| ---------------------- | -------------------------------------------------------------------- |
| group_id               | The unique group identifier.                                         |
| generation_id          | The generation of the group.                                         |
| member_id              | The member ID assigned by the group coordinator.                     |
| group_instance_id      | The unique identifier of the consumer instance provided by end user. |
| topics                 | The topics to commit offsets for.                                    |
| name                   | The topic name.                                                      |
| partitions             | Each partition to commit offsets for.                                |
| partition_index        | The partition index.                                                 |
| committed_offset       | The message offset to be committed.                                  |
| committed_leader_epoch | The leader epoch of this partition.                                  |
| committed_metadata     | Any associated metadata the client wants to keep.                    |
| _tagged_fields         | The tagged fields                                                    |
| _tagged_fields         | The tagged fields                                                    |
| _tagged_fields         | The tagged fields                                                    |

##### 响应

```
OffsetCommit Response (Version: 8) => throttle_time_ms [topics] TAG_BUFFER 
  throttle_time_ms => INT32
  topics => name [partitions] TAG_BUFFER 
    name => COMPACT_STRING
    partitions => partition_index error_code TAG_BUFFER 
      partition_index => INT32
      error_code => INT16ABLE_STRING
```

| FIELD            | DESCRIPTION                                                                                                                                  |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| throttle_time_ms | The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. |
| topics           | The responses for each topic.                                                                                                                |
| name             | The topic name.                                                                                                                              |
| partitions       | The responses for each partition in the topic.                                                                                               |
| partition_index  | The partition index.                                                                                                                         |
| error_code       | The error code, or 0 if there was no error.                                                                                                  |
| _tagged_fields   | The tagged fields                                                                                                                            |
| _tagged_fields   | The tagged fields                                                                                                                            |
| _tagged_fields   | The tagged fields                                                                                                                            |

## LEAVE_GROUP

##### 功能

退出消费组

##### 请求

```
LeaveGroup Request (Version: 5) => group_id [members] TAG_BUFFER 
  group_id => COMPACT_STRING
  members => member_id group_instance_id reason TAG_BUFFER 
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    reason => COMPACT_NULLABLE_STRING
```

| FIELD             | DESCRIPTION                                     |
| ----------------- | ----------------------------------------------- |
| group_id          | The ID of the group to leave.                   |
| members           | List of leaving member identities.              |
| member_id         | The member ID to remove from the group.         |
| group_instance_id | The group instance ID to remove from the group. |
| reason            | The reason why the member left the group.       |
| _tagged_fields    | The tagged fields                               |
| _tagged_fields    | The tagged fields                               |

##### 响应

```
LeaveGroup Response (Version: 5) => throttle_time_ms error_code [members] TAG_BUFFER 
  throttle_time_ms => INT32
  error_code => INT16
  members => member_id group_instance_id error_code TAG_BUFFER 
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    error_code => INT16
```

| throttle_time_ms  | The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| error_code        | The error code, or 0 if there was no error.                                                                                                  |
| members           | List of leaving member responses.                                                                                                            |
| member_id         | The member ID to remove from the group.                                                                                                      |
| group_instance_id | The group instance ID to remove from the group.                                                                                              |
| error_code        | The error code, or 0 if there was no error.                                                                                                  |
| _tagged_fields    | The tagged fields                                                                                                                            |
| _tagged_fields    | The tagged fields                                                                                                                            |
