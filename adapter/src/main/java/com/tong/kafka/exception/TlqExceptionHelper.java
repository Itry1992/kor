package com.tong.kafka.exception;

import com.tong.kafka.common.protocol.Errors;
import com.tong.kafka.manager.ITlqManager;
import com.tongtech.client.common.ClientErrorCode;
import com.tongtech.client.exception.TLQBrokerException;
import com.tongtech.client.exception.TLQClientException;
import com.tongtech.client.remoting.exception.RemotingConnectException;
import com.tongtech.client.remoting.exception.RemotingTooMuchRequestException;

public class TlqExceptionHelper {
    public static CommonKafkaException tlqExceptionConvert(Throwable e, ITlqManager manager, String... topics) {
        //RejectedExecutionException
        if (e instanceof RemotingConnectException) {
            return new CommonKafkaException(Errors.LEADER_NOT_AVAILABLE);
        }
        if (e instanceof RemotingTooMuchRequestException) {
            return new MessageTooLagerException();
        }
        if (e instanceof TLQClientException) {
            //路由查询时，尝试使用UNKNOWN_TOPIC_OR_PARTITION，而后kafka客户端会重新重新跟新Metadata
            //同时应当清楚对应的缓存
            if (((TLQClientException) e).getResponseCode() == ClientErrorCode.NOT_FIND_ROUTER) {
                if (topics != null && topics.length > 0) {
                    for (String topic : topics
                    ) {
                        manager.clearCache(topic);
                    }
                }
                return new CommonKafkaException(Errors.UNKNOWN_TOPIC_OR_PARTITION);
            }
        }
//        if (e instanceof RequestTimeoutException){
//
//        }
        if (e instanceof TLQBrokerException) {
            if (((TLQBrokerException) e).getResponseCode() == 100) {
                return new CommonKafkaException(Errors.LEADER_NOT_AVAILABLE, e, e.getMessage());

            }
            return new CommonKafkaException(Errors.UNKNOWN_SERVER_ERROR, e, e.getMessage());
        }
        if (e instanceof TLQClientException) {
            if (((TLQClientException) e).getResponseCode() == ClientErrorCode.MESSAGE_ILLEGAL) {
                return new MessageTooLagerException();
            }
        }
        return new CommonKafkaException(Errors.UNKNOWN_SERVER_ERROR, e, e.getMessage());
    }
}
