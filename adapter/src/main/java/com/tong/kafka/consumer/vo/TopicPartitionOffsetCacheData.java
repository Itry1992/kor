package com.tong.kafka.consumer.vo;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.protocol.Errors;

public class TopicPartitionOffsetCacheData extends TopicPartitionOffsetData {
    public TopicPartitionOffsetCacheData(TopicPartition topicPartition) {
        super(topicPartition);
        this.offsetCommitTime = 0L;
    }

    public TopicPartitionOffsetCacheData(TopicPartition topicPartition, Errors error) {
        super(topicPartition, error);
        this.offsetCommitTime = 0L;
    }

    private Long offsetCommitTime;

    public Long getOffsetCommitTime() {
        return offsetCommitTime;
    }

    public void setOffsetCommitTime(Long offsetCommitTime) {
        this.offsetCommitTime = offsetCommitTime;
    }
}
