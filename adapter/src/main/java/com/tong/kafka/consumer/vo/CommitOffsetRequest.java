package com.tong.kafka.consumer.vo;

import com.tong.kafka.common.TopicPartition;

public class CommitOffsetRequest {
    TopicPartition topicPartition;
    long commitOffset;
    long commitTime;

    public CommitOffsetRequest(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public CommitOffsetRequest setTopicPartition(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
        return this;
    }

    public long getCommitOffset() {
        return commitOffset;
    }

    public CommitOffsetRequest setCommitOffset(long commitOffset) {
        this.commitOffset = commitOffset;
        return this;
    }

    public long getCommitTime() {
        return commitTime;
    }

    public CommitOffsetRequest setCommitTime(long commitTime) {
        this.commitTime = commitTime;
        return this;
    }
}
