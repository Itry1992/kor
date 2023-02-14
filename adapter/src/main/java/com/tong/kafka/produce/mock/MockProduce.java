package com.tong.kafka.produce.mock;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.record.Record;
import com.tong.kafka.produce.AbsTlqProduce;
import com.tong.kafka.produce.KafkaRecordAttr;
import com.tong.kafka.produce.SendResult;
import com.tong.kafka.produce.exception.MessageTooLagerException;
import com.tongtech.client.message.Message;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MockProduce extends AbsTlqProduce {
    @Override
    public CompletableFuture<SendResult> sendBatch(TopicPartition tp, List<Record> records, KafkaRecordAttr messageAttr) throws MessageTooLagerException {
        List<Message> messages = buildBatchRecord(records, messageAttr, tp);

        System.out.println("accpet new message from " + tp + "size is " + messages.size());
        CompletableFuture<SendResult> sendResultCompletableFuture = new CompletableFuture<>();
        SendResult value = new SendResult();
        value.setOffset(0);
        value.setLogAppendTime(System.currentTimeMillis());
        sendResultCompletableFuture.complete(value);
        return sendResultCompletableFuture;
    }


    public static void main(String[] args) {
        MockProduce mockProduce = new MockProduce();
        try {
            mockProduce.sendBatch(new TopicPartition("1", 0), Collections.EMPTY_LIST, new KafkaRecordAttr());
        } catch (MessageTooLagerException e) {
            throw new RuntimeException(e);
        }
    }
}
