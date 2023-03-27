package com.tong.kafka.raft;

/**
 * 选举请求
 */
public class ElectionRequest {
    private int term;

    private int nodeId;
    private long timeMs;

    public ElectionRequest(int term, int nodeId, long time) {
        this.term = term;
        this.nodeId = nodeId;
        this.timeMs = time;
    }
}
