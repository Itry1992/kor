package com.tong.kafka.consumer;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.protocol.Errors;

import java.util.Map;

public class ConsumerGroupOffsetData {
    /**
     * 消费组id
     */
    private String groupId;
    /**
     * 组级别错误
     */
    private Errors error;
    /**
     * 主题分区数据 主题分区->主题分区offset
     */
    private Map<TopicPartition, TopicPartitionOffsetData> tpToOffsetDataMap;

    public ConsumerGroupOffsetData(String groupId) {
        this.groupId = groupId;
        error = Errors.NONE;
    }

    public String getGroupId() {
        return groupId;
    }

    public ConsumerGroupOffsetData setGroupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public Errors getError() {
        return error;
    }

    public ConsumerGroupOffsetData setError(Errors error) {
        this.error = error;
        return this;
    }

    public Map<TopicPartition, TopicPartitionOffsetData> getTpToOffsetDataMap() {
        return tpToOffsetDataMap;
    }

    public ConsumerGroupOffsetData setTpToOffsetDataMap(Map<TopicPartition, TopicPartitionOffsetData> tpToOffsetDataMap) {
        this.tpToOffsetDataMap = tpToOffsetDataMap;
        return this;
    }
}
