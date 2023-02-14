package com.tong.kafka.produce;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.record.Record;
import com.tong.kafka.produce.exception.MessageTooLagerException;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ITlqProduce {
    CompletableFuture<SendResult> sendBatch(TopicPartition tp, List<Record> records, KafkaRecordAttr messageAttr) throws MessageTooLagerException;
}
