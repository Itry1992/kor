package com.tong.kafka.manager.mock;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.manager.AbstractManager;
import com.tong.kafka.manager.vo.TlqBrokerNode;
import com.tong.kafka.manager.vo.TopicMetaData;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class MockManager extends AbstractManager {
    private MockData mockData = new MockData();

    @Override
    public CompletableFuture<Map<String, TopicMetaData>> getTopicMetaData(List<String> topicName) {
        HashMap<String, TopicMetaData> topicMetaDataMap = new HashMap<>();
        topicName.stream().map(this::getMetaData).filter(Optional::isPresent)
                .forEach(d -> {
                    topicMetaDataMap.put(d.get().getTopicName(), d.get());
                });
        return CompletableFuture.completedFuture(topicMetaDataMap);
    }

    @Override
    public CompletableFuture<Map<String, TopicMetaData>> getAllTopicMetaData() {
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
        CompletableFuture<Map<String, TopicMetaData>> topicMetaDataFuture = getTopicMetaData(Collections.singletonList(tp.topic()));
        try {
            Map<String, TopicMetaData> topicMetaData = topicMetaDataFuture.get();
            TlqBrokerNode tlqBrokerNode = topicMetaData.get(tp.topic()).getBind().get(tp.partition());
            return tlqBrokerNode != null;
        } catch (InterruptedException | ExecutionException e) {
            return false;
        }
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

    @Override
    public Optional<TlqBrokerNode> getTlqBrokerNode(TopicPartition topicPartition) {
        CompletableFuture<Map<String, TopicMetaData>> topicMetaDataFutrue = getTopicMetaData(Collections.singletonList(topicPartition.topic()));
        try {
            Map<String, TopicMetaData> topicMetaData = topicMetaDataFutrue.get();
            return Optional.ofNullable(topicMetaData.get(topicPartition.topic())).map(r -> r.getBind().get(topicPartition.partition()));
        } catch (InterruptedException | ExecutionException e) {
            return Optional.empty();
        }
    }
}
