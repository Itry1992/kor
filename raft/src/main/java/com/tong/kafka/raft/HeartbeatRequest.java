package com.tong.kafka.raft;

/**
 * 心跳发送请求
 */
public class HeartbeatRequest {
    private int term;
    private int nodeId;
    private int leaderId;
}
