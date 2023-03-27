package com.tong.kafka.raft;

import com.tong.kafka.common.requests.HeartbeatRequest;

import java.util.concurrent.CompletableFuture;

public interface RequestSender {
    CompletableFuture<ElectionResponse> sendElectionReq(int nodeId, ElectionRequest request);

    CompletableFuture<HeartbeatResponse> sendHeartbeat(int nodeId, HeartbeatRequest request);
}
