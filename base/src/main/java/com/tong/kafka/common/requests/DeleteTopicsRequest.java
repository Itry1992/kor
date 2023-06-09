/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tong.kafka.common.requests;

import com.tong.kafka.common.Uuid;
import com.tong.kafka.common.message.DeleteTopicsRequestData;
import com.tong.kafka.common.message.DeleteTopicsResponseData;
import com.tong.kafka.common.protocol.ApiKeys;
import com.tong.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class DeleteTopicsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<DeleteTopicsRequest> {
        private DeleteTopicsRequestData data;

        public Builder(DeleteTopicsRequestData data) {
            super(ApiKeys.DELETE_TOPICS);
            this.data = data;
        }

        @Override
        public DeleteTopicsRequest build(short version) {
            if (version >= 6 && !data.topicNames().isEmpty()) {
                data.setTopics(groupByTopic(data.topicNames()));
            }
            return new DeleteTopicsRequest(data, version);
        }
        
        private List<DeleteTopicsRequestData.DeleteTopicState> groupByTopic(List<String> topics) {
            List<DeleteTopicsRequestData.DeleteTopicState> topicStates = new ArrayList<>();
            for (String topic : topics) {
                topicStates.add(new DeleteTopicsRequestData.DeleteTopicState().setName(topic));
            }
            return topicStates;
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private DeleteTopicsRequestData data;

    private DeleteTopicsRequest(DeleteTopicsRequestData data, short version) {
        super(ApiKeys.DELETE_TOPICS, version);
        this.data = data;
    }

    @Override
    public DeleteTopicsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        DeleteTopicsResponseData response = new DeleteTopicsResponseData();
        if (version() >= 1) {
            response.setThrottleTimeMs(throttleTimeMs);
        }
        ApiError apiError = ApiError.fromThrowable(e);
        for (DeleteTopicsRequestData.DeleteTopicState topic : topics()) {
            response.responses().add(new DeleteTopicsResponseData.DeletableTopicResult()
                    .setName(topic.name())
                    .setTopicId(topic.topicId())
                    .setErrorCode(apiError.error().code()));
        }
        return new DeleteTopicsResponse(response);
    }
    
    public List<String> topicNames() {
        if (version() >= 6)
            return data.topics().stream().map(topic -> topic.name()).collect(Collectors.toList());
        return data.topicNames(); 
    }

    public int numberOfTopics() {
        if (version() >= 6)
            return data.topics().size();
        return data.topicNames().size();
    }
    
    public List<Uuid> topicIds() {
        if (version() >= 6)
            return data.topics().stream().map(topic -> topic.topicId()).collect(Collectors.toList());
        return Collections.emptyList();
    }
    
    public List<DeleteTopicsRequestData.DeleteTopicState> topics() {
        if (version() >= 6)
            return data.topics();
        return data.topicNames().stream().map(name -> new DeleteTopicsRequestData.DeleteTopicState().setName(name)).collect(Collectors.toList());
    }

    public static DeleteTopicsRequest parse(ByteBuffer buffer, short version) {
        return new DeleteTopicsRequest(new DeleteTopicsRequestData(new ByteBufferAccessor(buffer), version), version);
    }

}
