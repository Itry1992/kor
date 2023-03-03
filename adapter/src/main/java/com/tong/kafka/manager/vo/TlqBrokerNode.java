package com.tong.kafka.manager.vo;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TlqBrokerNode that = (TlqBrokerNode) o;
        return brokerId == that.brokerId && Objects.equals(port, that.port) && Objects.equals(addr, that.addr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(port, addr, brokerId);
    }
}
