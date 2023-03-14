package com.tong.kafka.tlq;

import com.tong.kafka.common.AdapterConfig;
import com.tong.kafka.common.AdapterScheduler;
import com.tongtech.client.admin.TLQManager;
import com.tongtech.client.common.Indicators;
import com.tongtech.client.consumer.topic.TLQTopicPullConsumer;
import com.tongtech.client.producer.topic.TLQTopicProducer;
import com.tongtech.slf4j.Logger;
import com.tongtech.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

public class TlqPool {
    private final String nameSrv;
    private final String domainName;
    private final int idleWaitingTime;
    private final AdapterScheduler executorService;
    private final int period;

    private final Pool<TLQTopicProducer, TlqProduceWrapper> producePool;
    private final Pool<TLQManager, TlqManagerWrapper> managerPool;
    private final Pool<TLQTopicPullConsumer, TlqConsumerWrapper> consumerPool;

    public TlqPool(AdapterScheduler executorService, AdapterConfig config) {
        this.nameSrv = config.getNameSrvProp();
        this.domainName = config.getDomainName();
        this.idleWaitingTime = config.getPoolIdleWaitingTimeMs();
        this.executorService = executorService;
        this.period = config.getPollPeriodMs();
        TlqProduceWrapper tlqProduceWrapper = new TlqProduceWrapper(this.nameSrv, config, this.domainName);
        this.producePool = new Pool<>(this.executorService, config.getPoolMaxProduceNums(), this.period, tlqProduceWrapper);
        TlqManagerWrapper managerWrapper = new TlqManagerWrapper(config, this.nameSrv, this.domainName);
        this.managerPool = new Pool<>(this.executorService, config.getPoolMaxManagerNums(), this.period, managerWrapper);
        TlqConsumerWrapper consumerWrapper = new TlqConsumerWrapper(this.nameSrv, config, this.domainName);
        this.consumerPool = new Pool<>(this.executorService, config.getPoolMaxConsumerNums(), this.period, consumerWrapper);
    }

    public Optional<TLQTopicProducer> getProducer() {
        return producePool.get();
    }

    public Optional<TLQManager> getManager() {
        return managerPool.get();
    }

    public Optional<TLQTopicPullConsumer> getConsumer() {
        return consumerPool.get();
    }


    public void close() {
        if (consumerPool != null) {
            this.consumerPool.close();
        }
        if (managerPool != null)
            this.managerPool.close();
        if (producePool != null)
            this.producePool.close();
    }

    private static class Pool<T2 extends Indicators, T extends AbsTlqWrapper<T2>> {
        Logger logger = LoggerFactory.getLogger(Pool.class);
        private final AdapterScheduler executorService;

        private final int maxNums;
        private volatile int currentIndex;

        private volatile boolean shouldAdd = false;

        private volatile boolean closed = false;

        private final List<T> tList = new CopyOnWriteArrayList<>();

        public Pool(AdapterScheduler executorService, int maxNums, int period, T first) {
            this.executorService = executorService;
            this.maxNums = maxNums;
            tList.add(first);
            this.executorService.schedule("tlq pool clear idle client", () -> {
                Iterator<T> iterator = tList.iterator();
                while (iterator.hasNext() && tList.size() > 1) {
                    T next = iterator.next();
                    if (next.maybeClear()) {
                        tList.remove(next);
                    }
                }
                if (shouldAdd && !closed) {
                    shouldAdd = false;
                    if (tList.size() < this.maxNums) {
                        try {
                            Optional<AbsTlqWrapper<T2>> newInstance = tList.get(0).getNewInstance();
                            if (!closed) {
                                newInstance.ifPresent(r -> tList.add((T) r));
                            } else {
                                newInstance.ifPresent(AbsTlqWrapper::clear);
                            }
                        } catch (Throwable e) {
                            logger.error("创建新实例失败", e);
                        }
                    }
                }
            }, 1000, period);
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
            int currentIndex = getCurrentIndex();
            this.currentIndex = (currentIndex + 1) % tList.size();
            return currentIndex;
        }

        private int getCurrentIndex() {
            return this.currentIndex % tList.size();
        }

        public void close() {
            closed = true;
            for (T next : tList) {
                next.clear();
                tList.remove(next);
            }
        }
    }

}

