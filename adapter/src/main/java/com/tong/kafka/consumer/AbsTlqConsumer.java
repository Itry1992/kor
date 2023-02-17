package com.tong.kafka.consumer;

import com.tong.kafka.common.CompletableFutureUtil;
import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.header.internals.RecordHeader;
import com.tong.kafka.common.protocol.ByteBufferAccessor;
import com.tong.kafka.common.record.*;
import com.tong.kafka.manager.ITlqManager;
import com.tong.kafka.manager.vo.TlqBrokerNode;
import com.tong.kafka.produce.vo.KafkaRecordAttr;
import com.tongtech.client.message.Message;
import com.tongtech.client.message.MessageExt;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class AbsTlqConsumer implements ITlqConsumer {
    private volatile long EXPECTED_REQUEST_CONSUMPTION_TIME = 3;
    protected ITlqManager manager;

    public AbsTlqConsumer(ITlqManager manager) {
        this.manager = manager;
    }

    public CompletableFuture<MemoryRecords> pullMessage(TopicPartition topicPartition, long offset, int maxWaitTime, int batchNum, int maxByte, int minByte) {
        Optional<TlqBrokerNode> node = manager.getTlqBrokerNode(topicPartition);
        if (node.isPresent()) {
//        CompletableFuture<List<MessageExt>> pullMessage = pullMessage(node, offset, maxWaitTime, topicPartition.topic(), batchNum);
//        return pullMessage.thenApply(messages -> messageToMemoryRecords(messages, maxByte));
            return pullMessageChain(node.get(), topicPartition.topic(), offset, maxWaitTime, batchNum, maxByte, minByte, new ArrayList<>(batchNum), System.currentTimeMillis());
        }
        return CompletableFuture.completedFuture(MemoryRecords.EMPTY);
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
        CompletableFuture<List<MessageExt>> pullMessageResult = pullMessage(node, offset, maxWaitTime, topic, batchNum);
        CompletableFutureUtil.completeTimeOut(pullMessageResult, Collections.emptyList(), maxWaitTime + (int) EXPECTED_REQUEST_CONSUMPTION_TIME, TimeUnit.MILLISECONDS);
        return pullMessageResult.thenCompose(messages -> {
            if (messages.isEmpty()) return CompletableFuture.completedFuture(MemoryRecords.EMPTY);
            MessageExt last = messages.get(messages.size() - 1);
            messages.addAll(0, lastMessages);
            MemoryRecords memoryRecords = messageToMemoryRecords(messages, maxBate);
            long useTime = System.currentTimeMillis() - beginTimes;
            int nextMaxWait = maxWaitTime - (int) useTime;
            EXPECTED_REQUEST_CONSUMPTION_TIME = (useTime + EXPECTED_REQUEST_CONSUMPTION_TIME) / 2;
            if (memoryRecords.sizeInBytes() < minByte && System.currentTimeMillis() - beginTimes < nextMaxWait - EXPECTED_REQUEST_CONSUMPTION_TIME) {
                return pullMessageChain(node, topic, last.getCommitLogOffset(), nextMaxWait, Math.min(2 * batchNum, 2000), maxBate, minByte, messages, System.currentTimeMillis());
            } else {
                return CompletableFuture.completedFuture(memoryRecords);
            }
        });

    }

    protected abstract CompletableFuture<List<MessageExt>> pullMessage(TlqBrokerNode node, long offset, int timeOut, String topic, int batchNum);


    protected MemoryRecords messageToMemoryRecords(List<MessageExt> messages, int maxByte) {
        messages = messages.stream().filter(messageExt -> {
            KafkaRecordAttr attr = KafkaRecordAttr.formMap(messageExt.getAttr());
            return attr.getMagic() != KafkaRecordAttr.INVALID_MAGIC;
        }).collect(Collectors.toList());
        if (messages.isEmpty()) return MemoryRecords.EMPTY;
        MessageExt headMessage = messages.get(0);
        long baseOffset = headMessage.getCommitLogOffset();
        SimpleRecord headRecord = messageToSimpleRecord(headMessage);
        int size = Math.max(maxByte, AbstractRecords.estimateSizeInBytesUpperBound(RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, headRecord.key(), headRecord.value(), headRecord.headers()));
        ByteBuffer buffer = ByteBuffer.allocate(size);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, TimestampType.LOG_APPEND_TIME, baseOffset, headMessage.getTime(), RecordBatch.NO_PRODUCER_ID, (short) RecordBatch.NO_PARTITION_LEADER_EPOCH, 0, false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH);

        for (MessageExt message : messages) {
            SimpleRecord record = messageToSimpleRecord(headMessage);
            if (builder.hasRoomFor(message.getTime(), record.key(), record.value(), record.headers()))
                builder.append(message.getTime(), record.key(), record.value(), record.headers());
            else break;
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
            int headerValueSize = bufferAccessor.readVarint();
            ByteBuffer headerValue = bufferAccessor.readByteBuffer(headerValueSize);

            recordHeaders[i] = new RecordHeader(headerKey, headerValue);
        }
        return new SimpleRecord(0, key, value, recordHeaders);
    }

}
