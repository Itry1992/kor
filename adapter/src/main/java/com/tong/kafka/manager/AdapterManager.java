package com.tong.kafka.manager;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.protocol.Errors;
import com.tong.kafka.exception.CommonKafkaException;
import com.tong.kafka.manager.vo.CacheTopicMetadata;
import com.tong.kafka.manager.vo.TlqBrokerNode;
import com.tong.kafka.manager.vo.TopicMetaData;
import com.tong.kafka.tlq.TlqPool;
import com.tongtech.client.admin.TLQManager;
import com.tongtech.client.exception.TLQClientException;
import com.tongtech.client.remoting.exception.RemotingException;
import com.tongtech.slf4j.Logger;
import com.tongtech.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class AdapterManager extends AbstractManager {
    Logger logger = LoggerFactory.getLogger(AdapterManager.class);
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


    public AdapterManager(TlqPool tlqPool, int topicMetadataAliveTime) {
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
        if (allAliveTime > System.currentTimeMillis()) {
            queryAll = true;
            doQuery();
            return CompletableFuture.completedFuture(getAllResultFromCache());
        }
        queryAll = true;
        return doQuery().thenCompose((v) -> {
            if (allAliveTime < System.currentTimeMillis()) {
                return getAllTopicMetaData();
            }
            HashMap<String, TopicMetaData> result = getAllResultFromCache();
            return CompletableFuture.completedFuture(result);
        });
    }

    private HashMap<String, TopicMetaData> getAllResultFromCache() {
        HashMap<String, TopicMetaData> result = new HashMap<>();
        cacheTopicMetadataMap.forEach((k, r) -> {
            Optional<TopicMetaData> topicMetaData = r.get(true);
            assert topicMetaData.isPresent();
            result.put(k, topicMetaData.get());
        });
        return result;
    }

    @Override
    public Optional<TlqBrokerNode> getTlqBrokerNode(TopicPartition topicPartition) {
        return cacheTopicMetadataMap.get(topicPartition.topic()).get(true)
                .map(r -> {
                    TlqBrokerNode tlqBrokerNode = r.getBind().get(topicPartition.partition());
                    if (tlqBrokerNode.getBrokerId() <= 0) {
                        clearCache(topicPartition.topic());
                        return null;
                    }
                    return tlqBrokerNode;
                });
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
        //查询之前重置带查询数据，查询期间其他线程可以添加待查询的数据
        synchronized (this) {
            if (queryIng) return queryIngFuture;
            Set<String> queryIngTopics_ = Collections.unmodifiableSet(queryIngTopics);
            boolean queryAll_ = queryAll;
            queryIng = true;
            queryIngTopics.clear();
            queryAll = false;

            ArrayList<String> queryTopics = new ArrayList<>(queryIngTopics_);
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
                        Integer brokerId = tp.getBrokerId();
                        if (brokerId <= 0) {
                            brokerId = -1;
                        }
                        node.setBrokerId(brokerId);
                        node.setAddr(tp.getAddr());
                        bind.put(tp.getPartition(), node);
                    });
                    topicMetaData.setBind(bind);
                    CacheTopicMetadata cacheTopicMetadata = new CacheTopicMetadata(topicMetadataAliveTime).setTopicMetaData(topicMetaData);
                    cacheTopicMetadataMap.put(k, cacheTopicMetadata);
                });
                if (queryAll_) {
                    allAliveTime = System.currentTimeMillis() + topicMetadataAliveTime;
                }
            }).whenComplete((v, ex) -> {
                synchronized (this) {
                    if (ex != null) {
                        logger.error("get topic metadata fail ,error {}", ex);
                        //如果查询失败，添加上次查询的数据
                        queryIngTopics.addAll(queryIngTopics_);
                        queryAll = queryAll_;
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

    @Override
    public boolean isDomainExist(String domainName) throws CommonKafkaException {
        TLQManager tlqManager = tlqPool.getManager().orElseThrow(() -> new CommonKafkaException(Errors.UNKNOWN_SERVER_ERROR));
        try {
            return tlqManager.queryDomainExist(domainName);
        } catch (RemotingException | InterruptedException | TLQClientException e) {
            throw new CommonKafkaException(Errors.UNKNOWN_SERVER_ERROR);
        }
    }
}
