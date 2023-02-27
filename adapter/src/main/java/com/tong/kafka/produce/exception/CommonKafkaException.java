package com.tong.kafka.produce.exception;

import com.tong.kafka.common.protocol.Errors;

public class CommonKafkaException extends Exception {
    private final Errors error;
    private Throwable cause;

    private String message;

    public CommonKafkaException(Errors error) {
        this.error = error;
    }

    public CommonKafkaException(Errors error, String message) {
        super(message);
        this.error = error;
        this.cause = cause;
        this.message = message;
    }


    public CommonKafkaException(Errors error, Throwable cause, String message) {
        super(message, cause);
        this.error = error;
        this.cause = cause;
        this.message = message;
    }


    public Errors getError() {
        return error;
    }
}
