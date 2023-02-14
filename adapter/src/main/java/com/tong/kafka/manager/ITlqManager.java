package com.tong.kafka.manager;

import java.util.List;
import java.util.Map;

public interface ITlqManager {
    /**
     * 获取主题的分区元数据
     *
     * @param topicName
     * @return
     */
    Map<String,TopicMetaData> getTopicMetaData(List<String> topicName);

    Map<String,TopicMetaData> getAllTopicMetaData();

    boolean hasTopic(String topicName);
}
