package com.tong.kafka.produce.vo;

public class SendResult {
    /**
     * 暂时返回0
     */
    private long offset;
    /**
     * 日志添加时间，暂时全部为当前时间
     */
    private long logAppendTime;
    /**
     * 对应kafka 分段文件起始offset，暂时全部为0
     */
    private long logStartOffset;

    public long getOffset() {
        return offset;
    }

    public SendResult setOffset(long offset) {
        this.offset = offset;
        return this;
    }

    public long getLogAppendTime() {
        return logAppendTime;
    }

    public SendResult setLogAppendTime(long logAppendTime) {
        this.logAppendTime = logAppendTime;
        return this;
    }

    public long getLogStartOffset() {
        return logStartOffset;
    }

    public SendResult setLogStartOffset(long logStartOffset) {
        this.logStartOffset = logStartOffset;
        return this;
    }
}
