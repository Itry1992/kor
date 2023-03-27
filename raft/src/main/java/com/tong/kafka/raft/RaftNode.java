package com.tong.kafka.raft;

import com.tong.kafka.common.AdapterScheduler;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftNode {
    private static final int INVALID_LEADER_ID = -1;
    private RaftStatus raftStatus;
    /**
     * 当期任期
     */
    private volatile int term = 0;
    private volatile int leaderId = INVALID_LEADER_ID;
    private volatile long nextVoteTime;

    private volatile boolean startIng = false;
    private volatile boolean started = false;

    private AtomicInteger voteGranted = new AtomicInteger(0);

    private final AdapterScheduler scheduler;
    private final int heartbeatInterval;

    private final int id;

    private final int[] otherNodeIds;

    private final RequestSender sender;

    private final Random random = new Random();  // 随机数生成器
    private final int halfNum;

    public RaftNode(AdapterScheduler scheduler, int heartbeatInterval, int id, int[] allNodeId, RequestSender sender) {
        this.scheduler = scheduler;
        this.heartbeatInterval = heartbeatInterval;
        this.id = id;
        this.sender = sender;
        this.otherNodeIds = Arrays.stream(allNodeId).filter(r -> r != id).toArray();
        halfNum = (int) Math.ceil((otherNodeIds.length + 1) >> 1);
    }

    public void startUp() {
        if (startIng)
            throw new RuntimeException("starting");
        if (started)
            throw new RuntimeException("started");
        startIng = true;
        this.scheduler.schedule("raft node heartbeat", this::heartbeat, heartbeatInterval * 2, heartbeatInterval);
    }

    //向管leader发送心跳，如果leader 返回了正确的心跳，重置选举计时器
    //如果不知道leader信息，向任意节点发送心跳请求，来获取正确的leader之后，重新发送
    private void heartbeat() {
        if (raftStatus.isLeader())
            return;
        if (leaderId == INVALID_LEADER_ID) {
            //向随机客户端发送请求
        }

    }

    private void sendHeartbeat(Integer nodeId) {
        //

    }

    private void election() {
        if (raftStatus.isLeader() || this.nextVoteTime < System.currentTimeMillis())
            return;
        //如果nextVoteTime 超时，向其他节点发起投票
        scheduler.scheduleOnce("send electionRequest to other node", () -> {
            this.term = term + 1;
            ElectionRequest electionRequest = new ElectionRequest(this.term, this.id, System.currentTimeMillis());
            voteGranted.set(0);
            Arrays.stream(otherNodeIds).forEach(e -> {
                this.sender.sendElectionReq(e, electionRequest).whenComplete((res, err) -> {
                    if (err != null) {
                        //todo 记录日志
                        return;
                    } else {
                        if (res.isVoteGranted()) {
                            int count = voteGranted.incrementAndGet();
                            if (count <= halfNum || this.raftStatus.isLeader())
                                return;
                            synchronized (this) {
                                //如果获得半数以上的选票
                                if (voteGranted.get() > halfNum && !this.raftStatus.isLeader()) {
                                    this.raftStatus = new RaftLeader();
                                    this.leaderId = this.id;
                                }
                            }
                        }

                    }
                    return;
                });
            });
        }, random.nextInt(150) + 150);

    }
}
