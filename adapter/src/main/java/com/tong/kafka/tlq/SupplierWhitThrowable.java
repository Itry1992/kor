package com.tong.kafka.tlq;

@FunctionalInterface
public interface SupplierWhitThrowable<T, TH extends Throwable> {
    T get() throws TH;
}
