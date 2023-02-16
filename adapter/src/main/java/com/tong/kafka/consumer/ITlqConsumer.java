package com.tong.kafka.consumer;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.record.MemoryRecords;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ITlqConsumer {
    /**
     * 查询以提交的offset
     *
     * @param groupMap 消费组->主题分区
     * @return 消费组->消费组内各个分区的committed_offset
     */
    Map<String, ConsumerGroupOffsetData> getCommittedOffset(Map<String, List<TopicPartition>> groupMap);

    /**
     * 查询主题分区的offset信息，
     *
     * @param requestMap 主题分区->查询信息
     * @return 主题分区->offset
     */
    Map<TopicPartition, TopicPartitionOffsetData> getTimestampOffset(Map<TopicPartition, TlqOffsetRequest> requestMap);

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

}
