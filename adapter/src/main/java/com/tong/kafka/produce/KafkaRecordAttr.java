package com.tong.kafka.produce;

import java.util.HashMap;
import java.util.Map;

/**
 * kafka record转换为 tlq message是，需要携带的自定义属性
 */
public class KafkaRecordAttr {
    //base offset  = tlq offset - offsetDelta
    private long offsetDelta;
    private long createTime;
    private int length;

    private int magic;

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


    public Map<String, String> toAttrMap() {
        HashMap<String, String> attrMap = new HashMap<>(4);
        attrMap.put("offsetDelta", String.valueOf(offsetDelta));
        attrMap.put("createTime", String.valueOf(createTime));
        attrMap.put("length", String.valueOf(length));
        attrMap.put("magic", String.valueOf(magic));
        return attrMap;
    }
    //最小
}
