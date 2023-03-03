package com.tong.kafka.tlq;

import com.tong.kafka.common.AdapterConfig;
import com.tongtech.client.consumer.topic.TLQTopicPullConsumer;
import com.tongtech.client.exception.TLQClientException;
import com.tongtech.slf4j.Logger;
import com.tongtech.slf4j.LoggerFactory;

import java.util.Optional;

public class TlqConsumerWrapper extends AbsTlqWrapper<TLQTopicPullConsumer> {
    private static Logger logger = LoggerFactory.getLogger(TlqConsumerWrapper.class);


    public TlqConsumerWrapper(String nameSrvAddr, AdapterConfig config, String domainName) {
        super(config);
        TLQTopicPullConsumer consumer = TlqFactory.createConsumer(nameSrvAddr, domainName);
        try {
            consumer.start();
            this.t = consumer;
            this.usable = true;
        } catch (TLQClientException e) {
            consumer.shutdown();
            logger.error("初始化消费者失败：");
            logger.error(e.getErrorMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public TLQTopicPullConsumer getProducer() {
        return t;
    }


    @Override
    void clear() {
        if (t != null && usable) {
            t.shutdown();
        }
    }

    @Override
    Optional<AbsTlqWrapper<TLQTopicPullConsumer>> getNewInstance() {

        TlqConsumerWrapper tlqProduceWrapper = null;
        try {
            tlqProduceWrapper = new TlqConsumerWrapper(this.getProducer().getNamesrvAddr(),
                    this.config,
                    this.getProducer().getDomain());
        } catch (Exception e) {
            logger.error("创建并启动生产者失败：", e.getMessage(), e);
        }
        return Optional.ofNullable(tlqProduceWrapper);
    }
}
