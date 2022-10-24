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

import com.tong.kafka.common.message.TxnOffsetCommitResponseData;
import com.tong.kafka.common.protocol.Errors;
import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.protocol.ApiKeys;
import com.tong.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Possible error codes:
 *
 *   - {@link Errors#INVALID_PRODUCER_EPOCH}
 *   - {@link Errors#NOT_COORDINATOR}
 *   - {@link Errors#COORDINATOR_NOT_AVAILABLE}
 *   - {@link Errors#COORDINATOR_LOAD_IN_PROGRESS}
 *   - {@link Errors#OFFSET_METADATA_TOO_LARGE}
 *   - {@link Errors#GROUP_AUTHORIZATION_FAILED}
 *   - {@link Errors#INVALID_COMMIT_OFFSET_SIZE}
 *   - {@link Errors#TRANSACTIONAL_ID_AUTHORIZATION_FAILED}
 *   - {@link Errors#REQUEST_TIMED_OUT}
 *   - {@link Errors#UNKNOWN_MEMBER_ID}
 *   - {@link Errors#FENCED_INSTANCE_ID}
 *   - {@link Errors#ILLEGAL_GENERATION}
 */
public class TxnOffsetCommitResponse extends AbstractResponse {

    private final TxnOffsetCommitResponseData data;

    public TxnOffsetCommitResponse(TxnOffsetCommitResponseData data) {
        super(ApiKeys.TXN_OFFSET_COMMIT);
        this.data = data;
    }

    public TxnOffsetCommitResponse(int requestThrottleMs, Map<TopicPartition, Errors> responseData) {
        super(ApiKeys.TXN_OFFSET_COMMIT);
        Map<String, TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic> responseTopicDataMap = new HashMap<>();

        for (Map.Entry<TopicPartition, Errors> entry : responseData.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            String topicName = topicPartition.topic();

            TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic topic = responseTopicDataMap.getOrDefault(
                topicName, new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic().setName(topicName));

            topic.partitions().add(new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                                       .setErrorCode(entry.getValue().code())
                                       .setPartitionIndex(topicPartition.partition())
            );
            responseTopicDataMap.put(topicName, topic);
        }

        data = new TxnOffsetCommitResponseData()
                   .setTopics(new ArrayList<>(responseTopicDataMap.values()))
                   .setThrottleTimeMs(requestThrottleMs);
    }

    @Override
    public TxnOffsetCommitResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(data.topics().stream().flatMap(topic ->
                topic.partitions().stream().map(partition ->
                        Errors.forCode(partition.errorCode()))));
    }

    public Map<TopicPartition, Errors> errors() {
        Map<TopicPartition, Errors> errorMap = new HashMap<>();
        for (TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic topic : data.topics()) {
            for (TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition partition : topic.partitions()) {
                errorMap.put(new TopicPartition(topic.name(), partition.partitionIndex()),
                             Errors.forCode(partition.errorCode()));
            }
        }
        return errorMap;
    }

    public static TxnOffsetCommitResponse parse(ByteBuffer buffer, short version) {
        return new TxnOffsetCommitResponse(new TxnOffsetCommitResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
