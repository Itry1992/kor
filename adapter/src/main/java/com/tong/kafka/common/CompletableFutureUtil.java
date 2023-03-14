package com.tong.kafka.common;

import java.util.concurrent.CompletableFuture;

public class CompletableFutureUtil {

    public static <T> void completeTimeOut(CompletableFuture<T> completableFuture, T value, int delay, AdapterScheduler adapterScheduler) {
        if (completableFuture.isDone() || completableFuture.isCancelled())
            return;
        adapterScheduler.scheduleOnce("CompletableFuture completeTimeOut", () -> {
                    if (completableFuture.isDone() || completableFuture.isCancelled())
                        return;
                    completableFuture.complete(value);
                }
                , delay);
    }

    public static <T> void completeExceptionallyTimeOut(CompletableFuture<T> completableFuture, Throwable ex, int delay, AdapterScheduler adapterScheduler) {
        if (completableFuture.isDone() || completableFuture.isCancelled())
            return;
        adapterScheduler.scheduleOnce("CompletableFuture completeExceptionallyTimeOut", () -> {
            if (completableFuture.isDone() || completableFuture.isCancelled())
                return;
            completableFuture.completeExceptionally(ex);
        }, delay);
    }
}
