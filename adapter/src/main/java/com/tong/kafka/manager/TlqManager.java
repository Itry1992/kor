package com.tong.kafka.manager;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.protocol.Errors;
import com.tong.kafka.exception.CommonKafkaException;
import com.tong.kafka.manager.vo.CacheTopicMetadata;
import com.tong.kafka.manager.vo.TlqBrokerNode;
import com.tong.kafka.manager.vo.TopicMetaData;
import com.tong.kafka.tlq.TlqPool;
import com.tongtech.client.admin.TLQManager;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class TlqManager extends AbstractManager {
    //cache topic,-> {broker_id,part_index}
    private volatile Set<String> queryIngTopics = new CopyOnWriteArraySet<>();
    private volatile CompletableFuture<Void> queryIngFuture = CompletableFuture.completedFuture(null);
    private volatile boolean queryIng = false;

    private volatile boolean queryAll = false;

    private volatile long allAliveTime;
    private final Map<String, CacheTopicMetadata> cacheTopicMetadataMap = new ConcurrentHashMap<>();
    private volatile boolean clearIng = false;

    private final TlqPool tlqPool;

    private final int topicMetadataAliveTime;


    public TlqManager(TlqPool tlqPool, int topicMetadataAliveTime) {
        this.tlqPool = tlqPool;
        this.topicMetadataAliveTime = topicMetadataAliveTime;
    }

    private void clearStaled() {
        if (clearIng) {
            return;
        }
        clearIng = true;
        cacheTopicMetadataMap.forEach((k, v) -> {
            if (!v.canGet()) {
                cacheTopicMetadataMap.remove(k);
            }
        });
        clearIng = false;
    }

    @Override
    public CompletableFuture<Map<String, TopicMetaData>> getTopicMetaData(List<String> topicNames) {
        clearStaled();
        HashMap<String, TopicMetaData> result = new HashMap<>();
        ArrayList<String> needUpdateTopics = new ArrayList<>();
        topicNames.forEach(topic -> {
            CacheTopicMetadata cacheTopicMetadata = cacheTopicMetadataMap.get(topic);
            Optional<TopicMetaData> topicMetaData = cacheTopicMetadata.get();
            if (cacheTopicMetadata == null || !topicMetaData.isPresent()) {
                if (queryIngTopics != null && !queryIngTopics.contains(topic)) {
                    needUpdateTopics.add(topic);
                }
            } else {
                result.put(topic, topicMetaData.get());
            }
        });
        if (needUpdateTopics.isEmpty()) return CompletableFuture.completedFuture(result);
        else {
            queryIngTopics.addAll(needUpdateTopics);
        }

        return doQuery().thenCompose((v) -> getTopicMetaData(topicNames));
    }

    @Override
    public CompletableFuture<Map<String, TopicMetaData>> getAllTopicMetaData() {
        queryAll = true;
        return doQuery().thenCompose((v) -> {
            if (allAliveTime < System.currentTimeMillis()) {
                return getAllTopicMetaData();
            }
            HashMap<String, TopicMetaData> result = new HashMap<>();
            cacheTopicMetadataMap.forEach((k, r) -> {
                Optional<TopicMetaData> topicMetaData = r.get(true);
                assert topicMetaData.isPresent();
                result.put(k, topicMetaData.get());
            });
            return CompletableFuture.completedFuture(result);
        });
    }

    @Override
    public Optional<TlqBrokerNode> getTlqBrokerNode(TopicPartition topicPartition) {
        return cacheTopicMetadataMap.get(topicPartition.topic()).get(true)
                .map(r -> r.getBind().get(topicPartition.partition()));
    }

    @Override
    public boolean hasTopic(String topicName) {
        return cacheTopicMetadataMap.containsKey(topicName);
    }

    @Override
    public boolean hasTopicPartition(TopicPartition tp) {
        if (hasTopic(tp.topic())) {
            return cacheTopicMetadataMap.get(tp.topic()).get(true).map(r -> r.getBind().containsKey(tp.partition())).orElse(false);
        }
        return false;
    }

    private CompletableFuture<Void> doQuery() {
        if (queryIng) return queryIngFuture;
        TLQManager tlqManager = null;
        try {
            tlqManager = tlqPool.getManager().orElseThrow(() -> new CommonKafkaException(Errors.LISTENER_NOT_FOUND));
        } catch (CommonKafkaException e) {
            queryIngFuture.completeExceptionally(e);
        }
        Set<String> queryIngTopics1 = Collections.unmodifiableSet(queryIngTopics);
        boolean queryAll1 = queryAll;
        //查询之前重置带查询数据，查询期间其他线程可以添加待查询的数据
        synchronized (this) {
            if (queryIng) return queryIngFuture;
            queryIng = true;
            queryIngTopics.clear();
            queryAll = false;

            ArrayList<String> queryTopics = new ArrayList<>(queryIngTopics1);
            assert tlqManager != null;
            CompletableFuture<List<com.tongtech.client.admin.TopicPartition>> completableFuture = tlqManager.partitionsFor(tlqManager.getDomain(), queryAll, queryTopics, 3000);
            queryIngFuture = completableFuture.thenAccept((result) -> {
                HashMap<String, List<com.tongtech.client.admin.TopicPartition>> topicToTp = new HashMap<>();
                result.forEach((r) -> {
                    List<com.tongtech.client.admin.TopicPartition> topicPartitions = topicToTp.get(r.getTopic());
                    if (topicPartitions == null) {
                        topicPartitions = new ArrayList<>();
                    }
                    if (topicPartitions.stream().noneMatch(l -> Objects.equals(l.getTopic(), r.getTopic()) && l.getPartition() == r.getPartition())) {
                        topicPartitions.add(r);
                    }
                    topicToTp.put(r.getTopic(), topicPartitions);
                });
                topicToTp.forEach((k, v) -> {
                    TopicMetaData topicMetaData = new TopicMetaData();
                    topicMetaData.setTopicName(k);
                    topicMetaData.setPartitionSize(v.size());
                    HashMap<Integer, TlqBrokerNode> bind = new HashMap<>();
                    v.forEach(tp -> {
                        TlqBrokerNode node = new TlqBrokerNode();
                        node.setBrokerId(tp.getBrokerId());
                        node.setAddr(tp.getAddr());
                        bind.put(tp.getPartition(), node);
                    });
                    topicMetaData.setBind(bind);
                    CacheTopicMetadata cacheTopicMetadata = new CacheTopicMetadata(topicMetadataAliveTime).setTopicMetaData(topicMetaData);
                    cacheTopicMetadataMap.put(k, cacheTopicMetadata);
                });
                if (queryAll1) {
                    allAliveTime = System.currentTimeMillis() + topicMetadataAliveTime;
                }
            }).whenComplete((v, ex) -> {
                synchronized (this) {
                    if (ex != null) {
                        //如果查询失败，添加上次查询的数据
                        queryIngTopics.addAll(queryIngTopics1);
                        queryAll = queryAll1;
                    }
                    queryIng = false;
                }
            });
            return queryIngFuture;
        }
    }

    @Override
    public void clearCache(String topic) {
        this.cacheTopicMetadataMap.remove(topic);
    }
}
