package com.tong.kafka.consumer;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.protocol.Errors;

public class TopicPartitionOffsetData {
    /**
     * -1
     */
    long offset;
    Errors error;

    public static long INVALID_OFFSET = -1;
    TopicPartition topicPartition;

    public TopicPartitionOffsetData(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
        error = Errors.NONE;
    }


    public long getOffset() {
        return offset;
    }

    public TopicPartitionOffsetData setOffset(long offset) {
        this.offset = offset;
        return this;
    }

    public Errors getError() {
        return error;
    }

    public void setError(Errors error) {
        this.error = error;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public TopicPartitionOffsetData setTopicPartition(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
        return this;
    }
}
