package com.tong.kafka.common;

public interface AdapterConfig {
    String NameSrvProp = "tlq.nameSvrAddr";
    String DomainName = "tlq.domain";
    String PoolIdleWaitingTime = "tlq.pool.idle.waiting.time";
    String PoolPeriodMs = "tlq.pool.period.ms";
    String PoolMaxManagerNums = "tlq.pool.max.manager.nums";
    String PollMaxConsumerNums = "tlq.pool.max.consumer.nums";
    String PollMaxProducerNums = "tlq.pool.max.producer.nums";

    String getNameSrvProp();

    String getDomainName();

    Integer getPoolIdleWaitingTimeMs();

    Integer getPollPeriodMs();

    Integer getPoolMaxConsumerNums();

    Integer getPoolMaxProduceNums();

    Integer getPoolMaxManagerNums();


}
