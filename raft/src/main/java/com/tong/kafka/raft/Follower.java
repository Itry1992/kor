package com.tong.kafka.raft;

public class Follower implements RaftStatus {
    @Override
    public boolean isLeader() {
        return false;
    }
}
