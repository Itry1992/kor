package com.tong.kafka.common;

public class Config implements AdapterConfig {
    @Override
    public String getNameSrvProp() {
        return "tcp://172.27.168.140:9888";
    }

    @Override
    public String getDomainName() {
        return "domain1";
    }

    @Override
    public Integer getPoolIdleWaitingTimeMs() {
        return 60 * 1000;
    }

    @Override
    public Integer getPollPeriodMs() {
        return 1000;
    }

    @Override
    public Integer getPoolMaxConsumerNums() {
        return 4;
    }

    @Override
    public Integer getPoolMaxProduceNums() {
        return 2;
    }

    @Override
    public Integer getPoolMaxManagerNums() {
        return 2;
    }
}
