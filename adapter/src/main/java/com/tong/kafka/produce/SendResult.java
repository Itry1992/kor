package com.tong.kafka.produce;

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
     * 对应kafka 分段文件其实时间，暂时全部为0
     */
    private long logStartOffset;

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getLogAppendTime() {
        return logAppendTime;
    }

    public void setLogAppendTime(long logAppendTime) {
        this.logAppendTime = logAppendTime;
    }

    public long getLogStartOffset() {
        return logStartOffset;
    }

    public void setLogStartOffset(long logStartOffset) {
        this.logStartOffset = logStartOffset;
    }
}
