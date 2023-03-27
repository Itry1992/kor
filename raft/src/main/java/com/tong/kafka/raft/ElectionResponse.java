package com.tong.kafka.raft;

public class ElectionResponse {
    private boolean voteGranted;

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }
}
