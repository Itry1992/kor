package com.tong.kafka.manager.vo;

import java.util.Map;

public class TopicMetaData {
    private String topicName;
    /**
     * 分区和broker绑定关系映射
     */
    private Map<Integer, TlqBrokerNode> bind;
    /**
     * 分区数量
     */
    private int partitionSize;


    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public Map<Integer, TlqBrokerNode> getBind() {
        return bind;
    }

    public void setBind(Map<Integer, TlqBrokerNode> bind) {
        this.bind = bind;
    }

    public int getPartitionSize() {
        return partitionSize;
    }

    public void setPartitionSize(int partitionSize) {
        this.partitionSize = partitionSize;
    }

    @Override
    public String toString() {
        return "TopicMetaData{" +
                "topicName='" + topicName + '\'' +
                ", bind=" + bind +
                ", partitionSize=" + partitionSize +
                '}';
    }

}
