package com.tong.kafka.consumer;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.protocol.Errors;
import com.tong.kafka.common.requests.OffsetFetchResponse;
import com.tong.kafka.consumer.vo.*;
import com.tong.kafka.exception.CommonKafkaException;
import com.tong.kafka.exception.TlqExceptionHelper;
import com.tong.kafka.manager.ITlqManager;
import com.tong.kafka.manager.vo.TlqBrokerNode;
import com.tong.kafka.tlq.TlqPool;
import com.tongtech.client.admin.OffsetAndTimestamp;
import com.tongtech.client.common.BrokerSelector;
import com.tongtech.client.common.DirectPullData;
import com.tongtech.client.consumer.PullCallback;
import com.tongtech.client.consumer.PullResult;
import com.tongtech.client.consumer.PullStatus;
import com.tongtech.client.consumer.common.TopicCommitOffset;
import com.tongtech.client.consumer.topic.TLQTopicPullConsumer;
import com.tongtech.client.exception.TLQBrokerException;
import com.tongtech.client.exception.TLQClientException;
import com.tongtech.client.message.MessageExt;
import com.tongtech.client.remoting.exception.RemotingException;
import com.tongtech.slf4j.Logger;
import com.tongtech.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AdapterConsumer extends AbsTlqConsumer {
    private static final Integer INVALID_TLQ_BROKER_ID = -1;
    Logger logger = LoggerFactory.getLogger(AdapterConsumer.class);
    private final TlqPool pool;

    private final ConcurrentHashMap<String, TopicPartitionOffsetCacheData> committedOffsetCache = new ConcurrentHashMap<>();

    public AdapterConsumer(ITlqManager manager, TlqPool pool) {
        super(manager);
        this.pool = pool;
    }

    @Override
    protected CompletableFuture<List<MessageExt>> pullMessage(TlqBrokerNode node, long offset, int timeOut, String topic, int batchNum) {
        CompletableFuture<List<MessageExt>> completableFuture = new CompletableFuture<>();
        Optional<TLQTopicPullConsumer> consumer = pool.getConsumer();
        if (!consumer.isPresent()) {
            completableFuture.completeExceptionally(new CommonKafkaException(Errors.LEADER_NOT_AVAILABLE));
            return completableFuture;
        }
        TLQTopicPullConsumer pullConsumer = consumer.get();
        DirectPullData directPullData = new DirectPullData();
        directPullData.setBrokerId(node.getBrokerId());
        directPullData.setTopicName(topic);
        try {
            pullConsumer.pullMessage(directPullData, offset, batchNum, new PullCallback() {
                @Override
                public void onSuccess(PullResult pullResult) {
                    if (pullResult.getPullStatus().equals(PullStatus.FOUND) && !pullResult.getMsgFoundList().isEmpty()) {
                        completableFuture.complete(pullResult.getMsgFoundList());
                    }
                    completableFuture.complete(Collections.emptyList());
                }

                @Override
                public void onException(Throwable e) {
                    completableFuture.completeExceptionally(TlqExceptionHelper.tlqExceptionConvert(e,manager,topic));
                }
            }, timeOut);
        } catch (TLQClientException | RemotingException | TLQBrokerException | InterruptedException e) {
            completableFuture.completeExceptionally(TlqExceptionHelper.tlqExceptionConvert(e,manager,topic));
        }
        return completableFuture;
    }

    private List<TopicPartition> getCommittedOffsetByCache(String groupId, List<TopicPartition> tps, Map<TopicPartition, TopicPartitionOffsetData> result) {
        List<TopicPartition> unCachedTps = new ArrayList<>();
        tps.forEach(tp -> {
            String commitCacheKey = getCommitCacheKey(groupId, tp);
            TopicPartitionOffsetCacheData cacheData = committedOffsetCache.get(commitCacheKey);
            if (cacheData == null) {
                unCachedTps.add(tp);
            } else {
                result.put(tp, cacheData);
            }
        });

        return unCachedTps;

    }

    @Override
    public CompletableFuture<ConsumerGroupOffsetData> getCommittedOffset(String groupId, List<TopicPartition> tps) {
        //返回的结果
        ConcurrentHashMap<TopicPartition, TopicPartitionOffsetData> tpToData = new ConcurrentHashMap<>();
        List<TopicPartition> unCachedTopicPartitions = getCommittedOffsetByCache(groupId, tps, tpToData);
        if (unCachedTopicPartitions.isEmpty()) {
            ConsumerGroupOffsetData value = new ConsumerGroupOffsetData(groupId);
            value.setTpToOffsetDataMap(tpToData);
            return CompletableFuture.completedFuture(value);
        }

        //根据主题分区对应的brokerId进行分组，方便后续对不同的broker发送消息
        Map<Integer, Set<TopicPartition>> brokerIdToTp = unCachedTopicPartitions.stream().collect(Collectors.groupingBy((r) -> manager.getTlqBrokerNode(r).map(TlqBrokerNode::getBrokerId).orElse(INVALID_TLQ_BROKER_ID), Collectors.toSet()));
        Optional<TLQTopicPullConsumer> optionalConsumer = pool.getConsumer();
        if (!optionalConsumer.isPresent()) {
            CompletableFuture<ConsumerGroupOffsetData> result = new CompletableFuture<>();
            result.completeExceptionally(new CommonKafkaException(Errors.LEADER_NOT_AVAILABLE));
            return result;
        }
        TLQTopicPullConsumer consumer = optionalConsumer.get();
        brokerIdToTp.entrySet().stream().filter(e -> Objects.equals(e.getKey(), INVALID_TLQ_BROKER_ID)).forEach(e -> {
            Set<TopicPartition> topicPartitionSet = e.getValue();
            topicPartitionSet.forEach(r -> tpToData.put(r, new TopicPartitionOffsetData(r, Errors.UNKNOWN_TOPIC_OR_PARTITION)));
        });
        Stream<CompletableFuture<Boolean>> futureStream = brokerIdToTp.entrySet().stream().filter(e -> !Objects.equals(e.getKey(), INVALID_TLQ_BROKER_ID)).map((r) -> {
            BrokerSelector brokerSelector = new BrokerSelector();
            Integer brokerId = r.getKey();
            brokerSelector.setBrokerId(brokerId);
            Set<TopicPartition> requestTps = r.getValue();
            List<String> topics = requestTps.stream().map(TopicPartition::topic).collect(Collectors.toList());
            logger.trace("向brokerId:{} 发起查询提交的offset,group:{}, topics:{},", groupId, brokerId, topics);
            return consumer.committedOffset(consumer.getDomain(), brokerSelector, groupId, topics, 3000)
                    .handle((res, e) -> {
                        //提前处理可能存在的错误
                        if (e != null) {
                            logger.error("向 brokerId:{} 查询提交的offset发生错误 error:{}", brokerId, e);
                            Errors error = TlqExceptionHelper.tlqExceptionConvert(e,manager, topics.toArray(new String[0])).getError();
                            requestTps.forEach(reqTp -> tpToData.put(reqTp, new TopicPartitionOffsetData(reqTp, error)));
                            return false;
                        }
                        requestTps.forEach(requestTp -> {
                            Optional<Map.Entry<com.tongtech.client.admin.TopicPartition, TopicCommitOffset>> resTp = res.entrySet().stream().filter(entry -> entry.getKey().getTopic().equals(requestTp.topic())).findAny();
                            if (resTp.isPresent() && resTp.get().getValue().getCommitOffset() >= 0) {
                                TopicPartitionOffsetData value = new TopicPartitionOffsetData(requestTp);
                                value.setOffset(resTp.get().getValue().getCommitOffset());
                                tpToData.put(requestTp, value);
                            } else {
                                TopicPartitionOffsetData value = new TopicPartitionOffsetData(requestTp);
                                value.setOffset(OffsetFetchResponse.INVALID_OFFSET);
                                tpToData.put(requestTp, value);
                            }
                        });
                        return true;
                    });
        });
        return CompletableFuture.allOf(futureStream.toArray(CompletableFuture[]::new)).thenApply((r) -> {
            ConsumerGroupOffsetData consumerGroupOffsetData = new ConsumerGroupOffsetData(groupId);
            consumerGroupOffsetData.setTpToOffsetDataMap(tpToData);
            return consumerGroupOffsetData;
        });
    }

    @Override
    public CompletableFuture<Map<TopicPartition, TopicPartitionOffsetData>> getTimestampOffset(Map<TopicPartition, TlqOffsetRequest> requestMap) {
        //根据主题分区对应的brokerId进行分组，方便后续对不同的broker发送消息
        Map<Integer, Set<TlqOffsetRequest>> brokerIdtoReq = requestMap.values().stream().collect(Collectors.groupingBy((r) -> manager.getTlqBrokerNode(r.getTopicPartition()).map(TlqBrokerNode::getBrokerId).orElse(INVALID_TLQ_BROKER_ID), Collectors.toSet()));
        Optional<TLQTopicPullConsumer> optionalConsumer = pool.getConsumer();
        if (!optionalConsumer.isPresent()) {
            CompletableFuture<Map<TopicPartition, TopicPartitionOffsetData>> result = new CompletableFuture<>();
            result.completeExceptionally(new CommonKafkaException(Errors.LEADER_NOT_AVAILABLE));
            return result;
        }
        TLQTopicPullConsumer consumer = optionalConsumer.get();
        ConcurrentHashMap<TopicPartition, TopicPartitionOffsetData> resultMap = new ConcurrentHashMap<>();
        Stream<CompletableFuture<?>> futureStream = brokerIdtoReq.entrySet().stream().map(entry -> {
            Set<TlqOffsetRequest> offsetRequests = entry.getValue();
            Integer brokerId = entry.getKey();
            if (Objects.equals(brokerId, INVALID_TLQ_BROKER_ID)) {
                offsetRequests.forEach(req -> {
                    TopicPartitionOffsetData value = new TopicPartitionOffsetData(req.getTopicPartition(), Errors.UNKNOWN_TOPIC_OR_PARTITION);
                    resultMap.put(req.getTopicPartition(), value);
                });
                return CompletableFuture.completedFuture(true);
            }
            //按type再次分组，分别发起请求
            Map<TlqOffsetRequest.Type, Set<TlqOffsetRequest>> typeToReqs = offsetRequests.stream().collect(Collectors.groupingBy(TlqOffsetRequest::getType, Collectors.toSet()));
            BrokerSelector brokerSelector = new BrokerSelector();
            brokerSelector.setBrokerId(brokerId);
            CompletableFuture<?>[] futures = typeToReqs.entrySet().stream().map(typeToReq -> {
                Set<TlqOffsetRequest> requestSet = typeToReq.getValue();
                CompletableFuture<Map<com.tongtech.client.admin.TopicPartition, OffsetAndTimestamp>> future = offsetQuery(typeToReq, consumer, brokerSelector);
                return future.handle((res, err) -> {
                    if (err != null) {
                        Errors error = TlqExceptionHelper.tlqExceptionConvert(err,manager,requestSet.stream().map(r->r.getTopicPartition().topic()).toArray(String[]::new)).getError();
                        requestSet.forEach((req) -> {
                            TopicPartitionOffsetData value = new TopicPartitionOffsetData(req.getTopicPartition());
                            value.setError(error);
                            resultMap.put(req.getTopicPartition(), value);
                        });
                        return false;
                    }
                    requestSet.forEach((req) -> {
                        TopicPartition topicPartition = req.getTopicPartition();
                        TopicPartitionOffsetData value = new TopicPartitionOffsetData(topicPartition);
                        Optional<Map.Entry<com.tongtech.client.admin.TopicPartition, OffsetAndTimestamp>> findInRes = res.entrySet().stream().filter(en -> isTopicEq(en.getKey(), topicPartition)).findAny();
                        if (findInRes.isPresent()) {
                            long offset = findInRes.get().getValue().getOffset();
                            value.setOffset(offset);
                            if (offset == TopicPartitionOffsetData.INVALID_OFFSET) {
                                value.setError(Errors.UNKNOWN_TOPIC_OR_PARTITION);
                            }
                        } else {
                            value.setOffset(TopicPartitionOffsetData.INVALID_OFFSET);
                            value.setError(Errors.UNKNOWN_SERVER_ERROR);
                        }
                    });
                    return true;
                });
            }).toArray(CompletableFuture[]::new);
            return CompletableFuture.allOf(futures);
        });
        return CompletableFuture.allOf(futureStream.toArray(CompletableFuture[]::new)).thenApply((r) -> resultMap);
    }

    private boolean isTopicEq(com.tongtech.client.admin.TopicPartition tp1, TopicPartition tp2) {
        return Objects.equals(tp1.getTopic(), tp2.topic());
    }


    @Override
    public CompletableFuture<Map<TopicPartition, Errors>> commitOffset(Map<TopicPartition, CommitOffsetRequest> offsetMap, String groupId) {

        ConcurrentHashMap<TopicPartition, Errors> resultMap = new ConcurrentHashMap<>();

        //根据主题分区对应的brokerId进行分组，方便后续对不同的broker发送消息
        Map<Integer, Set<Map.Entry<TopicPartition, CommitOffsetRequest>>> brokerIdToReqs = offsetMap.entrySet().stream().collect(Collectors.groupingBy((r) -> manager.getTlqBrokerNode(r.getKey()).map(TlqBrokerNode::getBrokerId).orElse(INVALID_TLQ_BROKER_ID), Collectors.toSet()));
        Optional<TLQTopicPullConsumer> consumerOpt = pool.getConsumer();
        if (!consumerOpt.isPresent()) {
            offsetMap.entrySet().forEach(e -> {
                resultMap.put(e.getKey(), Errors.LEADER_NOT_AVAILABLE);
            });
            return CompletableFuture.completedFuture(resultMap);
        }
        Stream<CompletableFuture<Boolean>> futureStream = brokerIdToReqs.entrySet().stream().map((en) -> {
            Integer brokerId = en.getKey();
            Set<Map.Entry<TopicPartition, CommitOffsetRequest>> tpToReq = en.getValue();
            if (Objects.equals(brokerId, INVALID_TLQ_BROKER_ID)) {
                tpToReq.forEach((k) -> resultMap.put(k.getKey(), Errors.UNKNOWN_TOPIC_OR_PARTITION));
                return CompletableFuture.completedFuture(false);
            }
            TLQTopicPullConsumer pullConsumer = consumerOpt.get();
            BrokerSelector brokerSelector = new BrokerSelector();
            brokerSelector.setBrokerId(brokerId);
            Map<String, TopicCommitOffset> query = tpToReq.stream().collect(Collectors.toMap(r -> r.getKey().topic(), r -> {
                TopicCommitOffset topicCommitOffset = new TopicCommitOffset();
                CommitOffsetRequest offsetRequest = r.getValue();
                topicCommitOffset.setCommitOffset(offsetRequest.getCommitOffset());
                topicCommitOffset.setCommitTime((int) (offsetRequest.getCommitTime() / 1000));
                return topicCommitOffset;
            }));
            return pullConsumer.commitOffset(pullConsumer.getDomain(), brokerSelector, groupId, query, 3000)
                    .handle((res, err) -> {
                        if (err != null) {
                            tpToReq.forEach((req) -> resultMap.put(req.getKey(), TlqExceptionHelper.tlqExceptionConvert(err,manager, query.keySet().toArray(new String[0])).getError()));
                            return false;
                        }
                        tpToReq.forEach((req) -> {
                            Optional<Map.Entry<com.tongtech.client.admin.TopicPartition, TopicCommitOffset>> topicRes = res.entrySet().stream().filter(resItem -> isTopicEq(resItem.getKey(), req.getKey())).findAny();
                            if (!topicRes.isPresent()) {
                                resultMap.put(req.getKey(), Errors.UNKNOWN_SERVER_ERROR);
                            } else {
                                TopicCommitOffset topicCommitOffset = topicRes.get().getValue();
                                if (topicCommitOffset.getCommitOffset() < 0) {
                                    resultMap.put(req.getKey(), Errors.UNKNOWN_SERVER_ERROR);
                                    return;
                                }
                                resultMap.put(req.getKey(), Errors.NONE);
                                //同时保存到本地缓存中
                                cacheCommitOffset(groupId, req, topicCommitOffset);
                            }
                        });
                        return true;
                    });
        });
        return CompletableFuture.allOf(futureStream.toArray(CompletableFuture[]::new)).thenApply((r) -> resultMap);
    }

    private void cacheCommitOffset(String groupId, Map.Entry<TopicPartition, CommitOffsetRequest> req, TopicCommitOffset topicCommitOffset) {
        String commitCacheKey = getCommitCacheKey(groupId, req.getKey());
        if (committedOffsetCache.get(commitCacheKey) == null || committedOffsetCache.get(commitCacheKey).getOffsetCommitTime() < topicCommitOffset.getCommitTime()) {
            TopicPartitionOffsetCacheData cacheData = new TopicPartitionOffsetCacheData(req.getKey());
            cacheData.setOffset(topicCommitOffset.getCommitOffset());
            cacheData.setOffsetCommitTime((long) topicCommitOffset.getCommitTime());
            committedOffsetCache.put(commitCacheKey, cacheData);
        }
    }


    private CompletableFuture<Map<com.tongtech.client.admin.TopicPartition, OffsetAndTimestamp>> offsetQuery(Map.Entry<TlqOffsetRequest.Type, Set<TlqOffsetRequest>> en, TLQTopicPullConsumer consumer, BrokerSelector brokerSelector) {
        TlqOffsetRequest.Type type = en.getKey();
        List<String> topics = en.getValue().stream().map(r -> r.getTopicPartition().topic()).collect(Collectors.toList());
        if (type == TlqOffsetRequest.Type.Start) {
            CompletableFuture<Map<com.tongtech.client.admin.TopicPartition, OffsetAndTimestamp>> future = consumer.beginningOffsets(consumer.getDomain(), brokerSelector, topics, 3000)
                    .thenApply(res ->
                            res.entrySet().stream()
                                    .collect(Collectors.toMap((e) -> e.getKey(),
                                            (e) -> new OffsetAndTimestamp(type.getValue(), e.getValue()))));
            return future;
        }
        if (type == TlqOffsetRequest.Type.End) {
            CompletableFuture<Map<com.tongtech.client.admin.TopicPartition, OffsetAndTimestamp>> future = consumer.endOffsets(consumer.getDomain(), brokerSelector, topics, 3000)
                    .thenApply(res ->
                            res.entrySet().stream()
                                    .collect(Collectors.toMap((e) -> e.getKey(),
                                            (e) -> new OffsetAndTimestamp(type.getValue(), e.getValue()))));
            return future;
        }

        Map<String, Integer> topicsReq = en.getValue().stream().collect(Collectors.toMap((e) -> e.getTopicPartition().topic(), (e) -> (int) (e.getTimestamp() / 1000)));
        return consumer.offsetsForTimes(consumer.getDomain(), brokerSelector, topicsReq, 3000);
    }

    private String getCommitCacheKey(String groupId, TopicPartition tp) {
        return groupId + ":" + tp.topic() + ":" + tp.partition();
    }

}
