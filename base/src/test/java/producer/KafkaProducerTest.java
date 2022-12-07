package producer;

import com.tong.kafka.clients.producer.KafkaProducer;
import com.tong.kafka.clients.producer.ProducerConfig;
import com.tong.kafka.clients.producer.ProducerRecord;
import com.tong.kafka.clients.producer.RecordMetadata;
import com.tong.kafka.common.record.CompressionType;
import com.tong.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

public class KafkaProducerTest implements Runnable {
    private final KafkaProducer<String, String> producer;
    private final String topic;


    public KafkaProducerTest(String topicName) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList("localhost:9999"));
        //acks=0：如果设置为0，生产者不会等待kafka的响应。
        //acks=1：这个配置意味着kafka会把这条消息写到本地日志文件中，但是不会等待集群中其他机器的成功响应。
        //acks=all：这个配置意味着leader会等待所有的follower同步完成。这个确保消息不会丢失，除非kafka集群中所有机器挂掉。这		是最强的可用性保证。
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //配置为大于0的值的话，客户端会在消息发送失败时重新发送。
//        props.put("retries", 0);
        //当多条消息需要发送到同一个分区时，生产者会尝试合并网络请求。
//        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,MyPartitioner.class.getName());
//        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.tong.kafka.clients.producer.RoundRobinPartitioner");
//        props.put(ProducerConfig.)
//        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"false");
        this.producer = new KafkaProducer<String, String>(props);
        this.topic = topicName;
    }


    public void run() {
        int count = 10000;
        CountDownLatch countDownLatch = new CountDownLatch(count);
        long l = System.currentTimeMillis();
        try {
            for (int i = 0; i < count; i++) {
                String messageStr = "message::hello" + i;
                ProducerRecord<String, String> message = new ProducerRecord<>(topic, "Message", messageStr);
                message.headers().add("aa", "cc".getBytes(StandardCharsets.UTF_8));
                Future<RecordMetadata> result = producer.send(message, (recordMetadata, e) -> {
//                    countDownLatch.countDown();
                });

                result.get();
//                if (i % 5 == 0)
//                    Thread.sleep(100);
            }
//            countDownLatch.await();
            System.out.println("end::" + (System.currentTimeMillis() - l));
        } catch (
                Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }

    public static void main(String args[]) {
        KafkaProducerTest test = new KafkaProducerTest("TOPIC_TEST");
        Thread thread = new Thread(test);
        thread.start();
    }
}