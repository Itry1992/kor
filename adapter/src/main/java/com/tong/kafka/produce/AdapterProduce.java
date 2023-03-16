package com.tong.kafka.produce;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.protocol.Errors;
import com.tong.kafka.common.record.Record;
import com.tong.kafka.exception.CommonKafkaException;
import com.tong.kafka.exception.TlqExceptionHelper;
import com.tong.kafka.manager.ITlqManager;
import com.tong.kafka.manager.vo.TlqBrokerNode;
import com.tong.kafka.produce.vo.KafkaRecordAttr;
import com.tong.kafka.produce.vo.SendResult;
import com.tong.kafka.tlq.TlqPool;
import com.tongtech.client.common.BrokerSelector;
import com.tongtech.client.message.Message;
import com.tongtech.client.producer.SendBatchCallback;
import com.tongtech.client.producer.SendBatchResult;
import com.tongtech.client.producer.SendStatus;
import com.tongtech.client.producer.topic.TLQTopicProducer;
import com.tongtech.slf4j.Logger;
import com.tongtech.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class AdapterProduce extends AbsTlqProduce {
    Logger logger = LoggerFactory.getLogger(AdapterProduce.class);
    private final TlqPool tlqPool;


    public AdapterProduce(TlqPool tlqPool, ITlqManager manager) {
        super(manager);
        this.tlqPool = tlqPool;
    }


    @Override
    public CompletableFuture<SendResult> sendBatch(TopicPartition tp, List<Record> records, KafkaRecordAttr messageAttr, int timeout) throws CommonKafkaException {
        TlqBrokerNode tlqBrokerNode = manager.getTlqBrokerNode(tp).orElseThrow(() -> new CommonKafkaException(Errors.UNKNOWN_TOPIC_OR_PARTITION));
        TLQTopicProducer tlqTopicProducer = tlqPool.getProducer().orElseThrow(() -> new CommonKafkaException(Errors.LEADER_NOT_AVAILABLE));
        List<Message> messages = buildBatchRecord(records, messageAttr, tp);
        BrokerSelector brokerSelector = new BrokerSelector();
        brokerSelector.setBrokerId(tlqBrokerNode.getBrokerId());
        CompletableFuture<SendResult> completableFuture = new CompletableFuture<>();
        try {
            logger.trace("send batch message to broker: {}, topic: {} , batch size: {} ", brokerSelector.getBrokerId(), messages.get(0).getTopicOrQueue(), messages.size());
            tlqTopicProducer.sendBatch(messages, brokerSelector, new SendBatchCallback() {
                @Override
                public void onSuccess(SendBatchResult sendBatchResult) {
                    if (sendBatchResult.getSendStatus().equals(SendStatus.SEND_OK)) {
                        logger.trace("send batch message to broker {} success", brokerSelector.getBrokerId());
                        completableFuture.complete(new SendResult().setLogAppendTime(System.currentTimeMillis()));
                    } else {
                        logger.error("send batch message to broker {} fail by unknown error");
                        completableFuture.completeExceptionally(new CommonKafkaException(Errors.LEADER_NOT_AVAILABLE));
                    }
                }

                @Override
                public void onException(Throwable throwable) {
                    logger.error("send batch message to broker {} fail due to error: {}", brokerSelector.getBrokerId(), throwable);
                    completableFuture.completeExceptionally(TlqExceptionHelper.tlqExceptionConvert(throwable, manager, tp.topic()));
                }
            }, timeout);
        } catch (Exception e) {
            logger.error("send batch message to broker {} fail due to error: {}", brokerSelector.getBrokerId(), e);
            completableFuture.completeExceptionally(TlqExceptionHelper.tlqExceptionConvert(e, manager, tp.topic()));
        }
        return completableFuture;
    }
}
