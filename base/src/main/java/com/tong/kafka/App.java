package com.tong.kafka;

import java.util.concurrent.CompletableFuture;

public class App {
    public static void main(String[] args) {

        CompletableFuture<String> future = new CompletableFuture<>();
        future.thenAccept((a) -> {
            System.out.println("xx3");
        });
        future.whenComplete((a, b) -> {
            System.out.println("c1");
        });
        future.whenComplete((a, b) -> {
            System.out.println("c2");
        });
        future.thenRun(() -> {
            System.out.println("xx4");
        });
        future.complete("a");
        System.out.println("he11");
    }
}
