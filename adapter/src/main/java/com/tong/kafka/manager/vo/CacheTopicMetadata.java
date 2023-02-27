package com.tong.kafka.manager.vo;

import java.util.Optional;

public class CacheTopicMetadata {
    private TopicMetaData topicMetaData;
    private long nextUpdateTime;


    public CacheTopicMetadata(long aliveTime) {
        this.nextUpdateTime = System.currentTimeMillis() + aliveTime;
    }

    public boolean canGet() {
        return nextUpdateTime >= System.currentTimeMillis();
    }

    public Optional<TopicMetaData> get() {
        return get(false);
    }

    public Optional<TopicMetaData> get(boolean force) {
        if (force)
            return Optional.of(topicMetaData);
        if (canGet())
            return Optional.ofNullable(topicMetaData);
        return Optional.empty();
    }

    public CacheTopicMetadata setTopicMetaData(TopicMetaData topicMetaData) {
        this.topicMetaData = topicMetaData;
        return this;
    }
}
