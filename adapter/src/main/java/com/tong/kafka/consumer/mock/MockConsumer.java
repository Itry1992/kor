package com.tong.kafka.consumer.mock;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.consumer.ConsumerGroupOffsetData;
import com.tong.kafka.consumer.ITlqConsumer;
import com.tong.kafka.consumer.TlqOffsetRequest;
import com.tong.kafka.consumer.TopicPartitionOffsetData;
import com.tong.kafka.produce.mock.MockProduce;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MockConsumer implements ITlqConsumer {
    private MockProduce produce;

    public MockConsumer(MockProduce produce) {
        this.produce = produce;
    }

    @Override
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
                topicPartitionLongHashMap.put(topicPartition, new TopicPartitionOffsetData(topicPartition).setOffset(TopicPartitionOffsetData.INVALID_OFFSET));
            }
            committedOffset.setTpToOffsetDataMap(topicPartitionLongHashMap);
            return committedOffset;
        }).collect(Collectors.toList());
    }


    @Override
    public Map<TopicPartition, TopicPartitionOffsetData> getTimestampOffset(Map<TopicPartition, TlqOffsetRequest> requestMap) {
        HashMap<TopicPartition, TopicPartitionOffsetData> result = new HashMap<>();
        requestMap.forEach((key, value) -> result.put(value.getTopicPartition(), new TopicPartitionOffsetData(key)));
        return result;
    }
}
