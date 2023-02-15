package com.tong.kafka.manager;

import com.tong.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

public interface ITlqManager {
    /**
     * 获取主题的分区元数据
     *
     * @param topicName
     * @return key: topicName;value: 绑定的分区信息
     */
    Map<String,TopicMetaData> getTopicMetaData(List<String> topicName);

    Map<String,TopicMetaData> getAllTopicMetaData();

    TlqBrokerNode getTlqBrokerNode(TopicPartition topicPartition);

    boolean hasTopic(String topicName);

    boolean hasTopicPartition(TopicPartition tp);


}
