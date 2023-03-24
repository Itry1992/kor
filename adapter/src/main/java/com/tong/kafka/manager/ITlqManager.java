package com.tong.kafka.manager;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.exception.CommonKafkaException;
import com.tong.kafka.manager.vo.TlqBrokerNode;
import com.tong.kafka.manager.vo.TopicMetaData;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * 获取主题分区绑定数据，尽可能从缓存中获取
 * kafka的思路是，如果有不存在的主题分区，则异步拉取后更新缓存，且会定时更新
 */
public interface ITlqManager {
    /**
     * 获取主题的分区元数据
     *
     * @param topicName
     * @return key: topicName;value: 绑定的分区信息
     */
    CompletableFuture<Map<String, TopicMetaData>> getTopicMetaData(List<String> topicName);

    /**
     * 获取全部主题的分区元数据
     *
     * @return
     */
    CompletableFuture<Map<String, TopicMetaData>> getAllTopicMetaData();

    /**
     * 获取分区对应的broker,应当为非阻塞方法
     *
     * @param topicPartition
     * @return
     */
    Optional<TlqBrokerNode> getTlqBrokerNode(TopicPartition topicPartition);

    /**
     * 判断是否包含改分区，应当为非阻塞方法，尽可能冲缓存获取。
     *
     * @param topicName
     * @return
     */
    boolean hasTopic(String topicName);

    /**
     * 判断是否包含改分区，尽可能冲缓存获取。
     *
     * @param tp
     * @return
     */
    boolean hasTopicPartition(TopicPartition tp);

    void clearCache(String topic);

    //todo 启动之前判断域名是否纯在
    //todo 现在对离线的broker会返回-1，需要特殊处理
    boolean isDomainExist(String domainName) throws CommonKafkaException;


}
