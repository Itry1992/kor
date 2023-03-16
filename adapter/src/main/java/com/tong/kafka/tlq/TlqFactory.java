package com.tong.kafka.tlq;

import com.tongtech.client.admin.TLQManager;
import com.tongtech.client.consumer.topic.TLQTopicPullConsumer;
import com.tongtech.client.exception.TLQClientException;
import com.tongtech.client.producer.topic.TLQTopicProducer;
import com.tongtech.slf4j.Logger;
import com.tongtech.slf4j.LoggerFactory;

public class TlqFactory {
    private static Logger logger = LoggerFactory.getLogger(TlqFactory.class);

    public static TLQTopicProducer createProducer(String namesrvAddr, String domain) {
        TLQTopicProducer tlqTopicProducer = new TLQTopicProducer();
        try {
            tlqTopicProducer.setNamesrvAddr(namesrvAddr);
            tlqTopicProducer.setDomain(domain);
        } catch (TLQClientException e) {
            logger.error(e.getErrorMessage(), e);
            throw new RuntimeException(e);
        }
        return tlqTopicProducer;
    }


    public static TLQTopicPullConsumer createConsumer(String namesrvAddr, String domain) {
        TLQTopicPullConsumer pullConsumer = new TLQTopicPullConsumer();
        try {
            pullConsumer.setNamesrvAddr(namesrvAddr);
            pullConsumer.setDomain(domain);
            pullConsumer.subscribe("topic0");
        } catch (TLQClientException e) {
            logger.error(e.getErrorMessage(), e);
            throw new RuntimeException(e);
        }
        return pullConsumer;
    }

    public static TLQManager createManager(String namesrvAddr, String domainName) {
        TLQManager tlqManager = new TLQManager();
        try {
            tlqManager.setNamesrvAddr(namesrvAddr);
            tlqManager.setDomain(domainName);
        } catch (TLQClientException e) {
            logger.error(e.getErrorMessage(), e);
            throw new RuntimeException(e);
        }
        return tlqManager;
    }
}
