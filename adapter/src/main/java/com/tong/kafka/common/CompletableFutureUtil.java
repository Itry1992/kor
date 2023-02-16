package com.tong.kafka.common;

import java.util.concurrent.*;

public class CompletableFutureUtil {
    private static final ScheduledExecutorService EXECUTOR_SERVICE = new ScheduledThreadPoolExecutor(1, new ThreadPoolExecutor.DiscardPolicy());

    public static <T> void completeTimeOut(CompletableFuture<T> completableFuture, T value, int delay, TimeUnit timeUnit) {
        if (completableFuture.isDone() || completableFuture.isCancelled())
            return;
        EXECUTOR_SERVICE.schedule(() -> {
                    if (completableFuture.isDone() || completableFuture.isCancelled())
                        return;
                    completableFuture.complete(value);
                }
                , delay, timeUnit);
    }

    public static <T> void completeExceptionallyTimeOut(CompletableFuture<T> completableFuture, Throwable ex, int delay, TimeUnit timeUnit) {
        if (completableFuture.isDone() || completableFuture.isCancelled())
            return;
        EXECUTOR_SERVICE.schedule(() -> {
            if (completableFuture.isDone() || completableFuture.isCancelled())
                return;
            completableFuture.completeExceptionally(ex);
        }, delay, timeUnit);
    }
}
