package transaction;

import com.tong.kafka.clients.consumer.*;
import com.tong.kafka.clients.producer.KafkaProducer;
import com.tong.kafka.clients.producer.ProducerRecord;
import com.tong.kafka.clients.producer.RecordMetadata;
import com.tong.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
//许多基于Kafka的应用，尤其是Kafka Stream应用中同时包含Consumer和Producer，前者负责从Kafka中获取消息，后者负责将处理完的数据写回Kafka的其它Topic中。

//为了实现该场景下的事务的原子性，Kafka需要保证对Consumer Offset的Commit与Producer对发送消息的Commit包含在同一个事务中。否则，如果在二者Commit中间发生异常
//kafka 通过 sendOffsetsToTransaction 来保证了该功能的实现
public class ConsumeTransformProduce {
    private static Properties getProducerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "47.52.199.51:9092");
        props.put("retries", 3); // 重试次数
        props.put("batch.size", 100); // 批量发送大小
        props.put("buffer.memory", 33554432); // 缓存大小，根据本机内存大小配置
        props.put("linger.ms", 1000); // 发送频率，满足任务一个条件发送
        props.put("client.id", "producer-syn-2"); // 发送端id,便于统计
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("transactional.id", "producer-2"); // 每台机器唯一
        props.put("enable.idempotence", true); // 设置幂等性
        return props;
    }

    private static Properties getConsumerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "47.52.199.51:9092");
        props.put("group.id", "test_3");
        props.put("session.timeout.ms", 30000);       // 如果其超时，将会可能触发rebalance并认为已经死去，重新选举Leader
        props.put("enable.auto.commit", "false");      // 开启自动提交
        props.put("auto.commit.interval.ms", "1000"); // 自动提交时间
        props.put("auto.offset.reset", "earliest"); // 从最早的offset开始拉取，latest:从最近的offset开始消费
        props.put("client.id", "producer-syn-1"); // 发送端id,便于统计
        props.put("max.poll.records", "100"); // 每次批量拉取条数
        props.put("max.poll.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"); // 设置隔离级别
        return props;
    }

    public static void main(String[] args) {
        // 创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProps());
        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProps());
        // 初始化事务
        producer.initTransactions();
        // 订阅主题
        consumer.subscribe(Arrays.asList("consumer-tran"));
        for (; ; ) {
            // 开启事务
            producer.beginTransaction();
            // 接受消息
            ConsumerRecords<String, String> records = consumer.poll(500);
            // 处理逻辑
            try {
                Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    // 处理消息
                    System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                    // 记录提交的偏移量
                    commits.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                    // 产生新消息
                    Future<RecordMetadata> metadataFuture = producer.send(new ProducerRecord<>("consumer-send", record.value() + "send"));
                }
                // 提交偏移量
                producer.sendOffsetsToTransaction(commits, new ConsumerGroupMetadata("consumerGroupId"));
                // 事务提交
                producer.commitTransaction();

            } catch (Exception e) {
                e.printStackTrace();
                producer.abortTransaction();
            }
        }
    }
}
