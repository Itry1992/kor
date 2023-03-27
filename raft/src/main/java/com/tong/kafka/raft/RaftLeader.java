package com.tong.kafka.raft;

public class RaftLeader implements RaftStatus {
    @Override
    public boolean isLeader() {
        return true;
    }
}
