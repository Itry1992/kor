package com.tong.kafka.manager.mock;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.manager.AbstractManager;
import com.tong.kafka.manager.TlqBrokerNode;
import com.tong.kafka.manager.TopicMetaData;

import java.util.*;

public class MockManager extends AbstractManager {
    private MockData mockData = new MockData();

    @Override
    public Map<String, TopicMetaData> getTopicMetaData(List<String> topicName) {
        HashMap<String, TopicMetaData> topicMetaDataMap = new HashMap<>();
        topicName.stream().map(this::getMetaData).filter(Optional::isPresent)
                .forEach(d -> {
                    topicMetaDataMap.put(d.get().getTopicName(), d.get());
                });
        return topicMetaDataMap;
    }

    @Override
    public Map<String, TopicMetaData> getAllTopicMetaData() {
        return getTopicMetaData(mockData.topics);
    }

    @Override
    public boolean hasTopic(String topicName) {
        return mockData.topics.contains(topicName);
    }

    @Override
    public boolean hasTopicPartition(TopicPartition tp) {
        if (!hasTopic(tp.topic()))
            return false;
        Map<String, TopicMetaData> topicMetaData = getTopicMetaData(Collections.singletonList(tp.topic()));
        TlqBrokerNode tlqBrokerNode = topicMetaData.get(tp.topic()).getBind().get(tp.partition());
        return tlqBrokerNode == null;
    }

    private Optional<TopicMetaData> getMetaData(String topicName) {
        if (!mockData.topics.contains(topicName)) {
            return Optional.empty();
        }
        TopicMetaData topicMetaData = new TopicMetaData();
        topicMetaData.setTopicName(topicName);
        topicMetaData.setPartitionSize(3);
        topicMetaData.setBind(mockData.getBindMap());
        return Optional.of(topicMetaData);
    }
}
