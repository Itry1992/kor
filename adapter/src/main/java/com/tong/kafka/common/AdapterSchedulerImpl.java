package com.tong.kafka.common;

import com.tong.kafka.common.utils.KafkaThread;
import com.tongtech.slf4j.Logger;
import com.tongtech.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class AdapterSchedulerImpl implements AdapterScheduler {
    Logger logger = LoggerFactory.getLogger(AdapterScheduler.class);
    private AtomicInteger schedulerThreadId = new AtomicInteger(0);
    private ScheduledThreadPoolExecutor executor = null;
    private final Integer threads;
    private String threadNamePrefix = "adapter-scheduler-";
    private boolean daemon = true;


    public AdapterSchedulerImpl(Integer threads, String threadNamePrefix, boolean daemon) {
        this.threads = threads;
        this.threadNamePrefix = threadNamePrefix;
        this.daemon = daemon;
    }
    @Override
    public synchronized boolean isStarted() {
        return executor != null;
    }

    public AdapterSchedulerImpl(Integer threads) {
        this.threads = threads;
    }

    @Override
    public synchronized void startup() {
        if (isStarted()) {
            throw new IllegalStateException("This scheduler has already been started!");
        }
        executor = new ScheduledThreadPoolExecutor(threads);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setRemoveOnCancelPolicy(true);
        executor.setThreadFactory(runnable ->
                new KafkaThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), runnable, daemon));
    }

    @Override
    public void shutdown() {
        ScheduledThreadPoolExecutor cachedExecutor = this.executor;
        if (cachedExecutor != null) {
            synchronized (this) {
                cachedExecutor.shutdown();
                this.executor = null;
            }
            try {
                cachedExecutor.awaitTermination(1, TimeUnit.HOURS);
            } catch (InterruptedException e) {
                //ignore
            }
        }
    }

    public ScheduledFuture<?> scheduleOnce(String name, Runnable command,Integer delay) {
        return schedule(name, command, delay, -1);
    }

    @Override
    public ScheduledFuture<?> schedule(String name, Runnable runnable, Integer delay, Integer period) {
        synchronized (this) {
            if (isStarted()) {
                Runnable runnable1 = () -> {
                    logger.trace("Beginning execution of scheduled task {}.", name);
                    try {
                        runnable.run();
                    } catch (Exception e) {
                        logger.error("Uncaught exception in scheduled task {} error:{}", name, e);
                    } finally {
                        logger.trace("Completed execution of scheduled task {}.", name);
                    }
                };

                if (period >= 0)
                    return executor.scheduleAtFixedRate(runnable1, delay, period, TimeUnit.MILLISECONDS);
                else
                    return executor.schedule(runnable1, delay, TimeUnit.MILLISECONDS);
            } else {
                logger.info("Kafka scheduler is not running at the time task {} is scheduled. The task is ignored.", name);
                return new NoOpScheduledFutureTask();
            }
        }
    }

    private class NoOpScheduledFutureTask implements ScheduledFuture<Void> {

        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed o) {
            return 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return true;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
    }
}
