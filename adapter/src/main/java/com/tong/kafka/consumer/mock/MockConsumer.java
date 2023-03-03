package com.tong.kafka.consumer.mock;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.protocol.Errors;
import com.tong.kafka.consumer.AbsTlqConsumer;
import com.tong.kafka.consumer.ITlqConsumer;
import com.tong.kafka.consumer.vo.CommitOffsetRequest;
import com.tong.kafka.consumer.vo.ConsumerGroupOffsetData;
import com.tong.kafka.consumer.vo.TlqOffsetRequest;
import com.tong.kafka.consumer.vo.TopicPartitionOffsetData;
import com.tong.kafka.manager.ITlqManager;
import com.tong.kafka.manager.vo.TlqBrokerNode;
import com.tong.kafka.produce.mock.MockProduce;
import com.tongtech.client.message.MessageExt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MockConsumer extends AbsTlqConsumer implements ITlqConsumer {
    private MockProduce produce;

    private Map<TopicPartition, Long> commitedOffsetMap = new ConcurrentHashMap<>();

    public MockConsumer(MockProduce produce, ITlqManager manager) {
        super(manager);
        this.produce = produce;
    }

    @Override
    public CompletableFuture<ConsumerGroupOffsetData> getCommittedOffset(String groupId, List<TopicPartition> tps) {
        HashMap<String, List<TopicPartition>> stringListHashMap = new HashMap<>();
        stringListHashMap.put(groupId, tps);
        return CompletableFuture.completedFuture(getCommittedOffset(stringListHashMap).get(groupId));
    }

    public Map<String, ConsumerGroupOffsetData> getCommittedOffset(Map<String, List<TopicPartition>> groupMap) {
        List<ConsumerGroupOffsetData> offsetFetchResults = getOffsetFetchResults(groupMap);
        HashMap<String, ConsumerGroupOffsetData> res = new HashMap<>(offsetFetchResults.size());
        offsetFetchResults.forEach((e) -> {
            ConsumerGroupOffsetData committedOffset = res.putIfAbsent(e.getGroupId(), e);
            if (committedOffset != null) {
                Map<TopicPartition, TopicPartitionOffsetData> offsetMap = committedOffset.getTpToOffsetDataMap();
                offsetMap.putAll(e.getTpToOffsetDataMap());
            }
        });
        return res;
    }


    private List<ConsumerGroupOffsetData> getOffsetFetchResults(Map<String, List<TopicPartition>> groupMap) {
        return groupMap.entrySet().stream().map(entry -> {
            List<TopicPartition> value = entry.getValue();
            String groupId = entry.getKey();
            ConsumerGroupOffsetData committedOffset = new ConsumerGroupOffsetData(groupId);
            HashMap<TopicPartition, TopicPartitionOffsetData> topicPartitionLongHashMap = new HashMap<>();
            for (TopicPartition topicPartition : value) {
                topicPartitionLongHashMap.put(topicPartition, new TopicPartitionOffsetData(topicPartition).setOffset(Optional.ofNullable(commitedOffsetMap.get(topicPartition)).orElse(TopicPartitionOffsetData.INVALID_OFFSET)));
            }
            committedOffset.setTpToOffsetDataMap(topicPartitionLongHashMap);
            return committedOffset;
        }).collect(Collectors.toList());
    }


    @Override
    public CompletableFuture<Map<TopicPartition, TopicPartitionOffsetData>> getTimestampOffset(Map<TopicPartition, TlqOffsetRequest> requestMap) {
        HashMap<TopicPartition, TopicPartitionOffsetData> result = new HashMap<>();
        requestMap.forEach((key, value) -> {
            TopicPartitionOffsetData offsetData = new TopicPartitionOffsetData(key);
            result.put(value.getTopicPartition(), offsetData);
        });
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public CompletableFuture<Map<TopicPartition, Errors>> commitOffset(Map<TopicPartition, CommitOffsetRequest> offsetMap, String groupId) {
        offsetMap.forEach((key, value) -> {
            commitedOffsetMap.put(key, value.getCommitOffset());
        });
        return CompletableFuture.completedFuture(null);
    }


    @Override
    protected CompletableFuture<List<MessageExt>> pullMessage(TlqBrokerNode node, long offset, int timeOut, String topic, int batchNum) {
        CompletableFuture<List<MessageExt>> listCompletableFuture = new CompletableFuture<>();
        listCompletableFuture.complete(produce.getCacheMessage());
        return listCompletableFuture;

    }
}
