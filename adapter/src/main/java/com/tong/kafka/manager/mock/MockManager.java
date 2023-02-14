package com.tong.kafka.manager.mock;

import com.tong.kafka.manager.AbstractManager;
import com.tong.kafka.manager.TopicMetaData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
