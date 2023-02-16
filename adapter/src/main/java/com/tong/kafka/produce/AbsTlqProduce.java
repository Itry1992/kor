package com.tong.kafka.produce;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.header.Header;
import com.tong.kafka.common.header.internals.RecordHeader;
import com.tong.kafka.common.record.Record;
import com.tong.kafka.common.utils.ByteUtils;
import com.tong.kafka.manager.ITlqManager;
import com.tong.kafka.manager.TlqBrokerNode;
import com.tong.kafka.manager.TopicMetaData;
import com.tongtech.client.message.Message;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public abstract class AbsTlqProduce implements ITlqProduce {

    private ITlqManager manager;

    public AbsTlqProduce(ITlqManager manager) {
        this.manager = manager;
    }

    public TlqBrokerNode getTlqBroker(TopicPartition topicPartition) {
        Map<String, TopicMetaData> topicMetaData = manager.getTopicMetaData(Collections.singletonList(topicPartition.topic()));
        return topicMetaData.get(topicPartition.topic()).getBind().get(topicPartition.partition());
    }


    protected List<Message> buildBatchRecord(List<Record> records, KafkaRecordAttr kafkaRecordAttr, TopicPartition topicPartition) {
        return records.stream().map(r -> {
            long offset = r.offset();
            KafkaRecordAttr attr = new KafkaRecordAttr(KafkaRecordAttr.INVALID_MAGIC)
                    .setMagic(kafkaRecordAttr.getMagic())
                    .setOffsetDelta((int) (offset))
                    .setCreateTime(r.timestamp());

            Message message = new Message();
            byte[] bodyFromRecord = getBodyFromRecord(r);
            message.setBody(bodyFromRecord);
            message.setTopicOrQueue(topicPartition.topic());
            attr.setLength(bodyFromRecord.length);
            message.setAttr(attr.toAttrMap());
            return message;
        }).collect(Collectors.toList());
    }

    private byte[] getBodyFromRecord(Record record) {
        //kafka 由于压缩算法和RecordV2的结构，每个record并不会直接记录其长度，只能依次读取,无法直接获取buffer,所以这里需要重新组装
        //保留感兴趣的部分，重新写入tlq的消息体中时，其格式为： keysize-key-vaulesize-value-headercount-headers
        //剩下的offset 位移, 基础时间戳和时间戳类型等，真实时间戳使用自定义属性进行记录
        int dataLength = 0;
        dataLength += ByteUtils.sizeOfVarint(record.keySize());
        dataLength += ByteUtils.sizeOfVarint(record.valueSize());
        dataLength += record.keySize() + record.valueSize();
        dataLength += ByteUtils.sizeOfVarint(record.headers().length);
        for (Header h1 : record.headers()) {
            RecordHeader header = (RecordHeader) h1;
            dataLength += ByteUtils.sizeOfVarint(header.getKeyBuffer().remaining());
            dataLength += ByteUtils.sizeOfVarint(Optional.ofNullable(header.getValueBuffer()).map(Buffer::remaining).orElse(0));
            dataLength += header.getKeyBuffer().remaining() + Optional.ofNullable(header.getValueBuffer()).map(r -> r.remaining()).orElse(0);
        }

        //key_length ->
        //key
        //value_length ->
        //value
        //header count ->
        //header key size
        //header key
        //header value size
        //header value
        byte[] bytes = new byte[dataLength];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        ByteUtils.writeVarint(record.keySize(), buffer);
        buffer.put(record.key());
        ByteUtils.writeVarint(record.valueSize(), buffer);
        buffer.put(record.value());
        ByteUtils.writeVarint(record.headers().length, buffer);
        Arrays.stream(record.headers()).forEach(h -> {
            RecordHeader header = (RecordHeader) h;
            ByteUtils.writeVarint(header.getKeyBuffer().remaining(), buffer);
            buffer.put(header.getKeyBuffer().slice());
            ByteBuffer headerValue = header.getValueBuffer();
            if (headerValue != null) {
                ByteUtils.writeVarint(headerValue.remaining(), buffer);
                buffer.put(header.getValueBuffer().slice());
            } else {
                ByteUtils.writeVarint(0, buffer);
            }
        });
        return bytes;
    }
}
