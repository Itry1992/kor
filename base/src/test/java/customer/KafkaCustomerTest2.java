package customer;

import com.tong.kafka.clients.consumer.*;
import com.tong.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class KafkaCustomerTest2 {
    private static KafkaConsumer<String, String> consumer;
    private static String topic = "TOPIC_TEST";

    static {
        consumer = initCustomer();
    }

    static private KafkaConsumer initCustomer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "tt_ttx");
        props.put("enable.auto.commit", false);
//        props.put("auto.commit.interval.ms", "10000");
        props.put("key.deserializer", "com.tong.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "com.tong.kafka.common.serialization.StringDeserializer");
        props.put("max.poll.records", 10);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString());
        return new KafkaConsumer(props);
    }

    public static void main(String[] args) {
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("onPartitionsRevoked");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("onPartitionsAssigned");
            }
        });
        try {
            int i = 0;
            while (true) {
                i++;
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
                if (!records.isEmpty()) {
                    if (i > 2 && i % 2 == 0) {
                        TopicPartition topic_test_0 = new TopicPartition("TOPIC_TEST", 0);
                        consumer.commitSync(Collections.singletonMap(topic_test_0, new OffsetAndMetadata(records.records(topic_test_0).get(0).offset() - 2)));
                    } else {
                        consumer.commitSync();
                    }

                }
//                consumer.partitionsFor(topic).forEach((tp) -> System.out.printf("%s - committed %s", tp, consumer.committed(new TopicPartition(tp.topic(), tp.partition()))));
//                if (!records.isEmpty()) {
//                    consumer.commitSync();
//
//                } else {
//                    consumer.commitSync(Collections.singletonMap(new TopicPartition("TOPIC_TEST",0),new OffsetAndMetadata(0)));
//                }
                Thread.sleep(1000);

            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
