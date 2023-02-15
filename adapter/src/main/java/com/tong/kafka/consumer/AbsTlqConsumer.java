package com.tong.kafka.consumer;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.header.internals.RecordHeader;
import com.tong.kafka.common.protocol.ByteBufferAccessor;
import com.tong.kafka.common.record.*;
import com.tong.kafka.manager.ITlqManager;
import com.tong.kafka.manager.TlqBrokerNode;
import com.tong.kafka.produce.KafkaRecordAttr;
import com.tongtech.client.message.Message;
import com.tongtech.client.message.MessageExt;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AbsTlqConsumer implements ITlqConsumer {
    private volatile long EXPECTED_REQUEST_CONSUMPTION_TIME = 3;
    ITlqManager manager;

    public CompletableFuture<MemoryRecords> pullMessage(TopicPartition topicPartition, long offset, int maxWaitTime, int batchNum, int maxByte, int minByte) {
        TlqBrokerNode node = manager.getTlqBrokerNode(topicPartition);
//        CompletableFuture<List<MessageExt>> pullMessage = pullMessage(node, offset, maxWaitTime, topicPartition.topic(), batchNum);
//        return pullMessage.thenApply(messages -> messageToMemoryRecords(messages, maxByte));
        return pullMessageChain(node, topicPartition.topic(), offset, maxWaitTime, batchNum, maxByte, minByte, new ArrayList<>(batchNum), System.currentTimeMillis());
    }

    /**
     * 如果不满足最小字节要求，会链式调用拉取消息，且下一次拉取消息数量翻倍
     *
     * @param node
     * @param topic
     * @param offset
     * @param maxWaitTime
     * @param batchNum
     * @param maxBate
     * @param minByte
     * @param lastMessages
     * @param beginTimes
     * @return
     */
    public CompletableFuture<MemoryRecords> pullMessageChain(TlqBrokerNode node, String topic, long offset, int maxWaitTime, int batchNum, int maxBate, int minByte, List<MessageExt> lastMessages, long beginTimes) {
        CompletableFuture<List<MessageExt>> pullMessage = pullMessage(node, offset, maxWaitTime, topic, batchNum);
        return pullMessage.thenApply(messages -> {
            if (messages.isEmpty())
                return MemoryRecords.EMPTY;
            MessageExt last = messages.get(messages.size() - 1);
            messages.addAll(0, lastMessages);
            MemoryRecords memoryRecords = messageToMemoryRecords(messages, maxBate);
            long useTime = System.currentTimeMillis() - beginTimes;
            int nextMaxWait = maxWaitTime - (int) useTime;
            EXPECTED_REQUEST_CONSUMPTION_TIME = (useTime + EXPECTED_REQUEST_CONSUMPTION_TIME) / 2;
            if (memoryRecords.sizeInBytes() < minByte && System.currentTimeMillis() - beginTimes < nextMaxWait - EXPECTED_REQUEST_CONSUMPTION_TIME) {
                CompletableFuture<MemoryRecords> memoryRecords1 = pullMessageChain(node, topic, last.getCommitLogOffset(), nextMaxWait, 2 * batchNum, maxBate, minByte, messages, System.currentTimeMillis());
                try {
                    return memoryRecords1.get(nextMaxWait, TimeUnit.MILLISECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    return memoryRecords;
                }
            } else
                return memoryRecords;
        });
    }

    protected abstract CompletableFuture<List<MessageExt>> pullMessage(TlqBrokerNode node, long offset, int timeOut, String topic, int batchNum);


    protected MemoryRecords messageToMemoryRecords(List<MessageExt> messages, int maxByte) {
        if (messages.isEmpty())
            return MemoryRecords.EMPTY;
        MessageExt headMessage = messages.get(0);
        long baseOffset = headMessage.getCommitLogOffset();
        SimpleRecord headRecord = messageToSimpleRecord(headMessage);
        int size = Math.max(maxByte, AbstractRecords.estimateSizeInBytesUpperBound(RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, headRecord.key(), headRecord.value(), headRecord.headers()));
        ByteBuffer buffer = ByteBuffer.allocate(size);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer,
                RecordBatch.MAGIC_VALUE_V2,
                CompressionType.NONE,
                TimestampType.LOG_APPEND_TIME,
                baseOffset,
                headMessage.getTime(),
                0L,
                (short) RecordBatch.NO_PARTITION_LEADER_EPOCH,
                0,
                false,
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH
        );

        for (MessageExt message : messages) {
            KafkaRecordAttr attr = KafkaRecordAttr.formMap(message.getAttr());
            SimpleRecord record = messageToSimpleRecord(headMessage);
            if (builder.hasRoomFor(message.getTime(), record.key(), record.value(), record.headers()))
                builder.append(message.getTime(), record.key(), record.value(), record.headers());
            else
                break;
        }
        builder.close();
        return builder.build();
    }


    protected SimpleRecord messageToSimpleRecord(Message message) {
        ByteBuffer buffer = ByteBuffer.wrap(message.getBody());
        ByteBufferAccessor bufferAccessor = new ByteBufferAccessor(buffer);
        //其格式为： keysize-key-vaulesize-value-headercount-headers
        int keySize = bufferAccessor.readVarint();
        ByteBuffer key = bufferAccessor.readByteBuffer(keySize);
        int valueSize = bufferAccessor.readVarint();
        ByteBuffer value = bufferAccessor.readByteBuffer(valueSize);
        int headerCount = bufferAccessor.readVarint();
        RecordHeader[] recordHeaders = new RecordHeader[headerCount];
        for (int i = 0; i < headerCount; i++) {
            int headerKeySize = bufferAccessor.readVarint();
            ByteBuffer headerKey = bufferAccessor.readByteBuffer(headerKeySize);
            int vauleSize = bufferAccessor.readVarint();
            ByteBuffer headerValue = bufferAccessor.readByteBuffer(valueSize);

            recordHeaders[i] = new RecordHeader(headerKey, vauleSize == 0 ? null : headerValue);
        }
        return new SimpleRecord(0, key, value, recordHeaders);
    }

}
