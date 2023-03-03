package com.tong.kafka.tlq;

import com.tong.kafka.common.AdapterConfig;
import com.tongtech.client.admin.TLQManager;
import com.tongtech.client.consumer.topic.TLQTopicPullConsumer;
import com.tongtech.client.producer.topic.TLQTopicProducer;
import com.tongtech.slf4j.Logger;
import com.tongtech.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TlqPool {
    private final String nameSrv;
    private final String domainName;
    private final int idleWaitingTime;
    private final ScheduledExecutorService executorService;
    private final int period;

    private final Pool<TLQTopicProducer, TlqProduceWrapper> produceWrapperPool;
    private final Pool<TLQManager, TlqManagerWrapper> managerPool;
    private final Pool<TLQTopicPullConsumer, TlqConsumerWrapper> consumerPool;

    public TlqPool(ScheduledExecutorService executorService, AdapterConfig config) {
        this.nameSrv = config.getNameSrvProp();
        this.domainName = config.getDomainName();
        this.idleWaitingTime = config.getPoolIdleWaitingTime();
        this.executorService = executorService;
        this.period = config.getPollPeriod();
        TlqProduceWrapper tlqProduceWrapper = new TlqProduceWrapper(this.nameSrv, config, this.domainName);
        this.produceWrapperPool = new Pool<>(this.executorService, config.getPoolMaxProduceNums(), this.period, tlqProduceWrapper);
        TlqManagerWrapper managerWrapper = new TlqManagerWrapper(config, this.nameSrv, this.domainName);
        this.managerPool = new Pool<>(this.executorService, config.getPoolMaxManagerNums(), this.period, managerWrapper);
        TlqConsumerWrapper consumerWrapper = new TlqConsumerWrapper(this.nameSrv, config, this.domainName);
        this.consumerPool = new Pool<>(this.executorService, config.getPoolMaxConsumerNums(), this.period, consumerWrapper);
    }

    public Optional<TLQTopicProducer> getProducer() {
        return produceWrapperPool.get();
    }

    public Optional<TLQManager> getManager() {
        return managerPool.get();
    }

    public Optional<TLQTopicPullConsumer> getConsumer() {
        return consumerPool.get();
    }

    private static class Pool<T2, T extends AbsTlqWrapper<T2>> {
        Logger logger = LoggerFactory.getLogger(Pool.class);
        private final ScheduledExecutorService executorService;

        private final int maxNums;
        private volatile int currentIndex;

        private volatile boolean shouldAdd = false;

        private final List<T> tList = new CopyOnWriteArrayList<>();

        public Pool(ScheduledExecutorService executorService, int maxNums, int period, T first) {
            this.executorService = executorService;
            this.maxNums = maxNums;
            tList.add(first);
            this.executorService.scheduleAtFixedRate(() -> {
                Iterator<T> iterator = tList.iterator();
                while (iterator.hasNext() && tList.size() > 1) {
                    T next = iterator.next();
                    if (next.maybeClear()) {
                        iterator.remove();
                    }
                }
                if (shouldAdd) {
                    shouldAdd = false;
                    if (tList.size() < this.maxNums) {
                        try {
                            Optional<T> newInstance = (Optional<T>) tList.get(0).getNewInstance();
                            newInstance.ifPresent(tList::add);
                        } catch (Throwable e) {
                            logger.error("创建新实例失败", e);
                        }
                    }
                }
            }, 0, period, TimeUnit.MILLISECONDS);
        }

        private Optional<T2> get() {
            int size = tList.size();
            for (int i = 0; i < size; i++) {
                int currentIndex = getCurrentIndex();
                T t = tList.get(currentIndex);
                boolean isLast = i == size - 1;
                Optional<T2> t1 = t.getT(isLast);
                if (t1.isPresent())
                    return t1;
                else
                    getAndIncrCurrentIndex();
                if (isLast) {
                    if (size < maxNums) shouldAdd = true;
                    return t1;
                }
            }
            return Optional.empty();
        }

        private int getAndIncrCurrentIndex() {
            int currentIndex = this.currentIndex % tList.size();
            this.currentIndex = (currentIndex + 1) % tList.size();
            return currentIndex;
        }

        private int getCurrentIndex() {
            return this.currentIndex % tList.size();
        }


    }

}

