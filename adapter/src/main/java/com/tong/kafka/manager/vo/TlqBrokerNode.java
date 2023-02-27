package com.tong.kafka.manager.vo;

public class TlqBrokerNode {
    private String port;
    private String addr;
    private int brokerId;

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    @Override
    public String toString() {
        return "BrokerNode{" +
                "port='" + port + '\'' +
                ", addr='" + addr + '\'' +
                ", brokerId='" + brokerId + '\'' +
                '}';
    }
}
