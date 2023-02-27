package com.tong.kafka.tlq;

import com.tong.kafka.common.protocol.Errors;
import com.tong.kafka.produce.exception.CommonKafkaException;
import com.tong.kafka.produce.exception.MessageTooLagerException;
import com.tongtech.client.common.ClientErrorCode;
import com.tongtech.client.exception.TLQClientException;
import com.tongtech.client.remoting.exception.RemotingConnectException;
import com.tongtech.client.remoting.exception.RemotingTooMuchRequestException;

public class TlqExceptionHelper {
    public static CommonKafkaException tlqExceptionConvert(Throwable e) {
        //RejectedExecutionException
        if (e instanceof RemotingConnectException) {
            return new CommonKafkaException(Errors.LEADER_NOT_AVAILABLE);
        }
        if (e instanceof RemotingTooMuchRequestException) {
            return new MessageTooLagerException();
        }
        if (e instanceof TLQClientException) {
            if (((TLQClientException) e).getResponseCode() == ClientErrorCode.MESSAGE_ILLEGAL) {
                return new MessageTooLagerException();
            }
        }
        return new CommonKafkaException(Errors.UNKNOWN_SERVER_ERROR, e, e.getMessage());
    }
}
