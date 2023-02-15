package com.tong.kafka.consumer;

import com.tong.kafka.common.TopicPartition;

import java.util.Arrays;

/**
 * 查询对应的offset
 */
public class TlqOffsetRequest {
    enum Type {
        Start(-2), End(-1), Timestamp(0);
        private long value;

        Type(long value) {
            this.value = value;
        }

        public static Type valueOf(long l) {
            return Arrays.stream(Type.values()).filter(r -> r.value == l).findFirst().orElse(Timestamp);
        }
    }

    TopicPartition topicPartition;
    long timestamp;

    Type type;

    public TlqOffsetRequest(TopicPartition topicPartition, long timestamp) {
        this.topicPartition = topicPartition;
        if (timestamp < -2) {
            timestamp = 0;
        }
        this.timestamp = timestamp;
        this.type = Type.valueOf(timestamp);
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public void setTopicPartition(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        if (timestamp < -2) {
            timestamp = 0;
        }
        this.timestamp = timestamp;
        this.type = Type.valueOf(timestamp);
    }
}
