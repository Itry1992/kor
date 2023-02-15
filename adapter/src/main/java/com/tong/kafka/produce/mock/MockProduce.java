package com.tong.kafka.produce.mock;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.record.Record;
import com.tong.kafka.manager.TlqBrokerNode;
import com.tong.kafka.manager.mock.MockManager;
import com.tong.kafka.produce.AbsTlqProduce;
import com.tong.kafka.produce.KafkaRecordAttr;
import com.tong.kafka.produce.SendResult;
import com.tong.kafka.produce.exception.MessageTooLagerException;
import com.tongtech.client.message.Message;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class MockProduce extends AbsTlqProduce {
    public MockProduce() {
        super(new MockManager());
    }

    private volatile List<Message> cacheMessage;

    private AtomicLong offsetCache = new AtomicLong();

    @Override
    public CompletableFuture<SendResult> sendBatch(TopicPartition tp, List<Record> records, KafkaRecordAttr messageAttr) throws MessageTooLagerException {
        List<Message> messages = buildBatchRecord(records, messageAttr, tp);
        TlqBrokerNode tlqBroker = getTlqBroker(tp);
        //
        cacheMessage = messages;
        System.out.println("accpet new message from " + tp + "size is " + messages.size());
        offsetCache.addAndGet(messages.size());
        CompletableFuture<SendResult> sendResultCompletableFuture = new CompletableFuture<>();
        SendResult value = new SendResult();
        value.setOffset(offsetCache.get());
        value.setLogAppendTime(System.currentTimeMillis());
        sendResultCompletableFuture.complete(value);
        return sendResultCompletableFuture;
    }


    public List<Message> getCacheMessage() {
        return cacheMessage;
    }
}
