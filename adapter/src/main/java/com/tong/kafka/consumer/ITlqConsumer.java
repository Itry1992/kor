package com.tong.kafka.consumer;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.protocol.Errors;
import com.tong.kafka.common.record.MemoryRecords;
import com.tong.kafka.consumer.vo.CommitOffsetRequest;
import com.tong.kafka.consumer.vo.ConsumerGroupOffsetData;
import com.tong.kafka.consumer.vo.TlqOffsetRequest;
import com.tong.kafka.consumer.vo.TopicPartitionOffsetData;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ITlqConsumer {
    /**
     * 查询以提交的offset
     *
     * @param
     * @return 消费组->消费组内各个分区的committed_offset
     */
    CompletableFuture<ConsumerGroupOffsetData> getCommittedOffset(String groupId, List<TopicPartition> tps);

    /**
     * 查询主题分区的offset信息，
     *
     * @param requestMap 主题分区->查询信息
     * @return 主题分区->offset
     */
    CompletableFuture<Map<TopicPartition, TopicPartitionOffsetData>> getTimestampOffset(Map<TopicPartition, TlqOffsetRequest> requestMap);

    /**
     * 向主题分区拉取信息，tlq目前不支持多主题拉取
     *
     * @param topicPartition
     * @param offset
     * @param maxWaitTime
     * @param batchNum
     * @param maxByte
     * @param minByte
     * @return
     */
    CompletableFuture<MemoryRecords> pullMessage(TopicPartition topicPartition, long offset, int maxWaitTime, int batchNum, int maxByte, int minByte);

    /**
     * 提交offset,
     *
     * @param offsetMap
     * @param groupId
     */
    CompletableFuture<Map<TopicPartition, Errors>> commitOffset(Map<TopicPartition, CommitOffsetRequest> offsetMap,String groupId);
}
