package com.tong.kafka.tlq;

import com.tong.kafka.common.AdapterConfig;
import com.tongtech.client.common.Indicators;
import com.tongtech.slf4j.Logger;
import com.tongtech.slf4j.LoggerFactory;

import java.util.Optional;

public abstract class AbsTlqWrapper<T extends Indicators> {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    protected T t;
    protected volatile boolean canUse = false;

    protected volatile long lastUsed = System.currentTimeMillis();

    protected volatile boolean usable;

    protected final AdapterConfig config;


    public AbsTlqWrapper(AdapterConfig config) {
        this.config = config;
    }


    /**
     * 通过限流算法来判断是否可以返回值
     *
     * @return
     */


    private boolean isBusy() {
        return this.t.getInFlightAsyncRequestsNum() < this.t.getMaxInFlightAsyncRequestsNum() * 0.9;
    }


    protected Optional<T> getT(boolean forceGet) {
        if (canUse) {
            return Optional.empty();
        }
        long now = System.currentTimeMillis();
        if (forceGet) {
            lastUsed = now;
            return Optional.of(t);
        }
        if (!isBusy()) {
            lastUsed = now;
            return Optional.of(t);
        }
        return Optional.empty();
    }


    abstract void clear();

    abstract Optional<AbsTlqWrapper<T>> getNewInstance();

    public boolean maybeClear() {
        if (!shouldClear()) {
            return false;
        }
        synchronized (this) {
            if (!shouldClear()) {
                return false;
            }

            clear();
            usable = false;
        }
        return true;
    }

    public boolean shouldClear() {
        return usable && lastUsed + config.getPoolIdleWaitingTimeMs() < System.currentTimeMillis()
                && t.getInFlightRequestsNum() == 0;
    }


}
