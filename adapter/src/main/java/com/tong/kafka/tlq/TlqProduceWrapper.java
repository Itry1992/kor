package com.tong.kafka.tlq;

import com.tong.kafka.common.AdapterConfig;
import com.tongtech.client.exception.TLQClientException;
import com.tongtech.client.producer.topic.TLQTopicProducer;
import com.tongtech.client.remoting.exception.RemotingConnectException;
import com.tongtech.client.remoting.exception.RemotingSendRequestException;
import com.tongtech.client.remoting.exception.RemotingTimeoutException;
import com.tongtech.slf4j.Logger;
import com.tongtech.slf4j.LoggerFactory;

import java.util.Optional;

public class TlqProduceWrapper extends AbsTlqWrapper<TLQTopicProducer> {
    private static Logger logger = LoggerFactory.getLogger(TlqProduceWrapper.class);


    public TlqProduceWrapper(String nameSrvAddr, AdapterConfig config, String domainName) {
        super(config);
        TLQTopicProducer producer = TlqFactory.createProducer(nameSrvAddr, domainName);
        try {
            producer.start();
            this.t = producer;
            this.usable = true;
        } catch (TLQClientException | InterruptedException | RemotingTimeoutException | RemotingSendRequestException |
                 RemotingConnectException e) {
            producer.shutdown();
            logger.error("初始化消费者失败：");
            logger.error("error :{}", e);
            throw new RuntimeException(e);
        }
    }

    public TLQTopicProducer getProducer() {
        return t;
    }


    @Override
    void clear() {
        if (t != null && usable) {
            t.shutdown();
        }
    }

    @Override
    Optional<AbsTlqWrapper<TLQTopicProducer>> getNewInstance() {

        TlqProduceWrapper tlqProduceWrapper = null;
        try {
            tlqProduceWrapper = new TlqProduceWrapper(this.getProducer().getNamesrvAddr(),
                    this.config,
                    this.getProducer().getDomain());
        } catch (Exception e) {
            logger.error("创建并启动生产者失败：", e.getMessage(), e);
        }
        return Optional.ofNullable(tlqProduceWrapper);
    }
}
