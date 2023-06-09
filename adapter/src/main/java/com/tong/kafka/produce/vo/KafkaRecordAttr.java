package com.tong.kafka.produce.vo;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * kafka record转换为 tlq message是，需要携带的自定义属性
 */
public class KafkaRecordAttr {
    //base offset  = tlq offset - offsetDelta
    private long offsetDelta;
    private long createTime;
    private int length;

    private int magic;


    public KafkaRecordAttr(int magic) {
        this.magic = magic;
    }

    public long getOffsetDelta() {
        return offsetDelta;
    }

    public KafkaRecordAttr setOffsetDelta(long offsetDelta) {
        this.offsetDelta = offsetDelta;
        return this;
    }

    public long getCreateTime() {
        return createTime;
    }

    public KafkaRecordAttr setCreateTime(long createTime) {
        this.createTime = createTime;
        return this;
    }

    public int getLength() {
        return length;
    }

    public KafkaRecordAttr setLength(int length) {
        this.length = length;
        return this;
    }

    public int getMagic() {
        return magic;
    }

    public KafkaRecordAttr setMagic(int magic) {
        this.magic = magic;
        return this;

    }

    public static final String OFFSET_DELTA = "offsetDelta";
    public static final String CREATE_TIME = "createTime";
    public static final String LENGTH = "length";
    public static final String MAGIC = "kafka_magic";

    public static final Integer INVALID_MAGIC = -1;

    public Map<String, String> toAttrMap() {
        HashMap<String, String> attrMap = new HashMap<>(4);
        attrMap.put(OFFSET_DELTA, String.valueOf(offsetDelta));
        attrMap.put(CREATE_TIME, String.valueOf(createTime));
        attrMap.put(LENGTH, String.valueOf(length));
        attrMap.put(MAGIC, String.valueOf(magic));
        return attrMap;
    }

    public static KafkaRecordAttr formMap(Map<String, String> attr) {
        return new KafkaRecordAttr(INVALID_MAGIC)
                .setMagic(Integer.parseInt(Optional.ofNullable(attr.get(MAGIC)).orElse(INVALID_MAGIC.toString())))
                .setLength(Integer.parseInt(Optional.ofNullable(attr.get(LENGTH)).orElse("0")))
                .setCreateTime(Long.parseLong(Optional.ofNullable(attr.get(CREATE_TIME)).orElse("0")))
                .setOffsetDelta(Long.parseLong(Optional.ofNullable(attr.get(OFFSET_DELTA)).orElse("0")));
    }
    //最小
}
