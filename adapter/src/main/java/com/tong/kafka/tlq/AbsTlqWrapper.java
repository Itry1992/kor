package com.tong.kafka.tlq;

import com.tong.kafka.common.AdapterConfig;
import com.tongtech.slf4j.Logger;
import com.tongtech.slf4j.LoggerFactory;

import java.util.Optional;

public abstract class AbsTlqWrapper<T> {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    protected T t;
    protected volatile boolean canUse = false;
    private final int slotSize;
    private final int timeWindow;
    //仅仅使用了volatile 而非AtomicInteger 是认为并发带来的误差是可以接受的
    private volatile int currentSlotIndex;
    protected volatile boolean usable;

    private final int idleWaitingTime;

    protected final AdapterConfig config;

    private final long[] timeSlot;


    public int getIdleWaitingTime() {
        return idleWaitingTime;
    }

    public AbsTlqWrapper(AdapterConfig config) {
        this.idleWaitingTime = config.getPoolIdleWaitingTime();
        this.config = config;
        this.slotSize = 8;
        timeWindow = 4;
        timeSlot = new long[this.slotSize];
    }

    private int getAndIncCurrentSlot() {
        int currentSlotIndex1 = (currentSlotIndex + 1) % slotSize;
        currentSlotIndex = currentSlotIndex1;
        return currentSlotIndex1;
    }

    /**
     * 通过限流算法来判断是否可以返回值
     *
     * @return
     */
    protected boolean canGetAndRecord(long now) {
        int currentSlot = getAndIncCurrentSlot();
        timeSlot[currentSlot] = now;
        return canGet(now, currentSlot);
    }

    private boolean canGet(long now) {
        return canGet(now, currentSlotIndex);
    }

    private boolean canGet(long now, int slotIndex) {
        if (now - getLastSlot(slotIndex) > timeWindow) return true;
        return false;
    }

    private long getLastSlot(int currentSlotIndex) {
        return timeSlot[(currentSlotIndex + slotSize - 1) % slotSize];
    }

    protected Optional<T> getT(boolean forceGet) {
        if (canUse) {
            return Optional.empty();
        }
        long now = System.currentTimeMillis();
        if (forceGet) {
            canGetAndRecord(now);
            return Optional.of(t);
        }
        if (canGet(now)) {
            canGetAndRecord(now);
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

    public long getLastUsed() {
        return timeSlot[currentSlotIndex];
    }

    public boolean shouldClear() {
        return usable && getLastUsed() + idleWaitingTime < System.currentTimeMillis();
    }


}
