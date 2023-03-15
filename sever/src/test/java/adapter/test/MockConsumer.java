package adapter.test;

import com.tong.kafka.common.AdapterScheduler;
import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.protocol.Errors;
import com.tong.kafka.consumer.AdapterConsumer;
import com.tong.kafka.consumer.vo.AdapterOffsetRequest;
import com.tong.kafka.consumer.vo.CommitOffsetRequest;
import com.tong.kafka.consumer.vo.ConsumerGroupOffsetData;
import com.tong.kafka.consumer.vo.TopicPartitionOffsetData;
import com.tong.kafka.manager.ITlqManager;
import com.tong.kafka.tlq.TlqPool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class MockConsumer extends AdapterConsumer {

    public MockConsumer(ITlqManager manager, TlqPool pool, AdapterScheduler scheduler) {
        super(manager, pool, scheduler);
    }

    @Override
    public CompletableFuture<ConsumerGroupOffsetData> getCommittedOffset(String groupId, List<TopicPartition> tps) {
        ConsumerGroupOffsetData consumerGroupOffsetData = new ConsumerGroupOffsetData(groupId);
        Map<TopicPartition, TopicPartitionOffsetData> result = tps.stream().map(tp -> {
            TopicPartitionOffsetData topicPartitionOffsetData = new TopicPartitionOffsetData(tp);
            topicPartitionOffsetData.setOffset(-1);
            return topicPartitionOffsetData;
        }).collect(Collectors.toMap(TopicPartitionOffsetData::getTopicPartition, r -> r));
        consumerGroupOffsetData.setTpToOffsetDataMap(result);
        return CompletableFuture.completedFuture(consumerGroupOffsetData);
    }

    @Override
    public CompletableFuture<Map<TopicPartition, TopicPartitionOffsetData>> getTimestampOffset(Map<TopicPartition, AdapterOffsetRequest> requestMap) {
        Map<TopicPartition, TopicPartitionOffsetData> dataMap = requestMap.entrySet().stream().map((e) -> {
            TopicPartition tp = e.getKey();
            TopicPartitionOffsetData topicPartitionOffsetData = new TopicPartitionOffsetData(tp);
            topicPartitionOffsetData.setOffset(1);
            return topicPartitionOffsetData;
        }).collect(Collectors.toMap(TopicPartitionOffsetData::getTopicPartition, r -> r));
        return CompletableFuture.completedFuture(dataMap);
    }

    @Override
    public CompletableFuture<Map<TopicPartition, Errors>> commitOffset(Map<TopicPartition, CommitOffsetRequest> offsetMap, String groupId) {
        Map<TopicPartition, Errors> collect = offsetMap.keySet().stream().collect(Collectors.toMap(r -> r, r -> Errors.NONE));
        return CompletableFuture.completedFuture(collect);
    }
}
