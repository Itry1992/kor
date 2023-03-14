package com.tong.kafka.tlq.test;

import com.tong.kafka.common.AdapterConfig;
import com.tong.kafka.common.AdapterScheduler;
import com.tong.kafka.common.AdapterSchedulerImpl;
import com.tong.kafka.common.Config;
import com.tong.kafka.tlq.TlqPool;
import com.tongtech.client.admin.TLQManager;
import com.tongtech.client.consumer.topic.TLQTopicPullConsumer;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PoolTest {
    AdapterConfig config = new Config();
    AdapterScheduler adapterScheduler = new AdapterSchedulerImpl(1);

    @Test
    public void createPool() throws InterruptedException {
        TlqPool tlqPool = new TlqPool(adapterScheduler, config);
        Optional<TLQManager> manager = tlqPool.getManager();
        Optional<TLQTopicPullConsumer> consumer = tlqPool.getConsumer();
        assertTrue(manager.isPresent());
        assertTrue(consumer.isPresent());
        TLQManager tlqManager = manager.get();
        TLQTopicPullConsumer pullConsumer = consumer.get();
        assertEquals(tlqManager.getInFlightRequestsNum(), 0);
        assertEquals(0, pullConsumer.getInFlightRequestsNum());
        Thread.sleep(config.getPoolIdleWaitingTimeMs() + 1);
        consumer = tlqPool.getConsumer();
        assertTrue(consumer.isPresent());
        tlqPool.close();
    }
}
