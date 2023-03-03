package com.tong.kafka.exception;

import com.tong.kafka.common.protocol.Errors;

//MESSAGE_TOO_LARGE
public class MessageTooLagerException extends CommonKafkaException {
    public MessageTooLagerException() {
        super(Errors.MESSAGE_TOO_LARGE);
    }

    public MessageTooLagerException(Errors error, String message) {
        super(error, message);
    }

    public MessageTooLagerException(Errors error, Throwable cause, String message) {
        super(error, cause, message);
    }
}
