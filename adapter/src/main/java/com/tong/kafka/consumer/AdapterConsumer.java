package com.tong.kafka.consumer;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.protocol.Errors;
import com.tong.kafka.consumer.vo.CommitOffsetRequest;
import com.tong.kafka.consumer.vo.ConsumerGroupOffsetData;
import com.tong.kafka.consumer.vo.TlqOffsetRequest;
import com.tong.kafka.consumer.vo.TopicPartitionOffsetData;
import com.tong.kafka.manager.ITlqManager;
import com.tong.kafka.manager.vo.TlqBrokerNode;
import com.tongtech.client.message.MessageExt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class AdapterConsumer extends AbsTlqConsumer{
    public AdapterConsumer(ITlqManager manager) {
        super(manager);
    }

    @Override
    protected CompletableFuture<List<MessageExt>> pullMessage(TlqBrokerNode node, long offset, int timeOut, String topic, int batchNum) {
        return null;
    }

    @Override
    public CompletableFuture<ConsumerGroupOffsetData> getCommittedOffset(String groupId, List<TopicPartition> tps) {
        return null;
    }

    @Override
    public CompletableFuture<HashMap<TopicPartition, TopicPartitionOffsetData>> getTimestampOffset(Map<TopicPartition, TlqOffsetRequest> requestMap) {
        return null;
    }

    @Override
    public CompletableFuture<Map<TopicPartition, Errors>> commitOffset(Map<TopicPartition, CommitOffsetRequest> offsetMap) {
        return null;
    }
}
