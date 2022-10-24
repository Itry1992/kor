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

import com.tong.kafka.common.IsolationLevel;
import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.message.ListOffsetsRequestData;
import com.tong.kafka.common.protocol.ApiKeys;
import com.tong.kafka.common.protocol.ByteBufferAccessor;
import com.tong.kafka.common.protocol.Errors;
import com.tong.kafka.common.message.ListOffsetsResponseData;
import com.tong.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import com.tong.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;

import java.nio.ByteBuffer;
import java.util.*;

public class ListOffsetsRequest extends AbstractRequest {
    public static final long EARLIEST_TIMESTAMP = -2L;
    public static final long LATEST_TIMESTAMP = -1L;
    public static final long MAX_TIMESTAMP = -3L;

    public static final int CONSUMER_REPLICA_ID = -1;
    public static final int DEBUGGING_REPLICA_ID = -2;

    private final ListOffsetsRequestData data;
    private final Set<TopicPartition> duplicatePartitions;

    public static class Builder extends AbstractRequest.Builder<ListOffsetsRequest> {
        private final ListOffsetsRequestData data;

        public static Builder forReplica(short allowedVersion, int replicaId) {
            return new Builder((short) 0, allowedVersion, replicaId, IsolationLevel.READ_UNCOMMITTED);
        }

        public static Builder forConsumer(boolean requireTimestamp, IsolationLevel isolationLevel, boolean requireMaxTimestamp) {
            short minVersion = 0;
            if (requireMaxTimestamp)
                minVersion = 7;
            else if (isolationLevel == IsolationLevel.READ_COMMITTED)
                minVersion = 2;
            else if (requireTimestamp)
                minVersion = 1;
            return new Builder(minVersion, ApiKeys.LIST_OFFSETS.latestVersion(), CONSUMER_REPLICA_ID, isolationLevel);
        }

        private Builder(short oldestAllowedVersion,
                        short latestAllowedVersion,
                        int replicaId,
                        IsolationLevel isolationLevel) {
            super(ApiKeys.LIST_OFFSETS, oldestAllowedVersion, latestAllowedVersion);
            data = new ListOffsetsRequestData()
                      .setIsolationLevel(isolationLevel.id())
                      .setReplicaId(replicaId);
        }

        public Builder setTargetTimes(List<ListOffsetsRequestData.ListOffsetsTopic> topics) {
            data.setTopics(topics);
            return this;
        }

        @Override
        public ListOffsetsRequest build(short version) {
            return new ListOffsetsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    /**
     * Private constructor with a specified version.
     */
    private ListOffsetsRequest(ListOffsetsRequestData data, short version) {
        super(ApiKeys.LIST_OFFSETS, version);
        this.data = data;
        duplicatePartitions = new HashSet<>();
        Set<TopicPartition> partitions = new HashSet<>();
        for (ListOffsetsRequestData.ListOffsetsTopic topic : data.topics()) {
            for (ListOffsetsRequestData.ListOffsetsPartition partition : topic.partitions()) {
                TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                if (!partitions.add(tp)) {
                    duplicatePartitions.add(tp);
                }
            }
        }
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        short errorCode = Errors.forException(e).code();

        List<ListOffsetsTopicResponse> responses = new ArrayList<>();
        for (ListOffsetsRequestData.ListOffsetsTopic topic : data.topics()) {
            ListOffsetsTopicResponse topicResponse = new ListOffsetsTopicResponse().setName(topic.name());
            List<ListOffsetsPartitionResponse> partitions = new ArrayList<>();
            for (ListOffsetsRequestData.ListOffsetsPartition partition : topic.partitions()) {
                ListOffsetsPartitionResponse partitionResponse = new ListOffsetsPartitionResponse()
                        .setErrorCode(errorCode)
                        .setPartitionIndex(partition.partitionIndex());
                if (versionId == 0) {
                    partitionResponse.setOldStyleOffsets(Collections.emptyList());
                } else {
                    partitionResponse.setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
                                     .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP);
                }
                partitions.add(partitionResponse);
            }
            topicResponse.setPartitions(partitions);
            responses.add(topicResponse);
        }
        ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setTopics(responses);
        return new ListOffsetsResponse(responseData);
    }

    @Override
    public ListOffsetsRequestData data() {
        return data;
    }

    public int replicaId() {
        return data.replicaId();
    }

    public IsolationLevel isolationLevel() {
        return IsolationLevel.forId(data.isolationLevel());
    }

    public List<ListOffsetsRequestData.ListOffsetsTopic> topics() {
        return data.topics();
    }

    public Set<TopicPartition> duplicatePartitions() {
        return duplicatePartitions;
    }

    public static ListOffsetsRequest parse(ByteBuffer buffer, short version) {
        return new ListOffsetsRequest(new ListOffsetsRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static List<ListOffsetsRequestData.ListOffsetsTopic> toListOffsetsTopics(Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> timestampsToSearch) {
        Map<String, ListOffsetsRequestData.ListOffsetsTopic> topics = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> entry : timestampsToSearch.entrySet()) {
            TopicPartition tp = entry.getKey();
            ListOffsetsRequestData.ListOffsetsTopic topic = topics.computeIfAbsent(tp.topic(), k -> new ListOffsetsRequestData.ListOffsetsTopic().setName(tp.topic()));
            topic.partitions().add(entry.getValue());
        }
        return new ArrayList<>(topics.values());
    }

    public static ListOffsetsRequestData.ListOffsetsTopic singletonRequestData(String topic, int partitionIndex, long timestamp, int maxNumOffsets) {
        return new ListOffsetsRequestData.ListOffsetsTopic()
                .setName(topic)
                .setPartitions(Collections.singletonList(new ListOffsetsRequestData.ListOffsetsPartition()
                        .setPartitionIndex(partitionIndex)
                        .setTimestamp(timestamp)
                        .setMaxNumOffsets(maxNumOffsets)));
    }
}
