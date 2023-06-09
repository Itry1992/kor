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

import com.tong.kafka.common.message.TxnOffsetCommitRequestData;
import com.tong.kafka.common.message.TxnOffsetCommitResponseData;
import com.tong.kafka.common.protocol.ByteBufferAccessor;
import com.tong.kafka.common.protocol.Errors;
import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.errors.UnsupportedVersionException;
import com.tong.kafka.common.protocol.ApiKeys;
import com.tong.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class TxnOffsetCommitRequest extends AbstractRequest {

    private final TxnOffsetCommitRequestData data;

    public static class Builder extends AbstractRequest.Builder<TxnOffsetCommitRequest> {

        public final TxnOffsetCommitRequestData data;


        public Builder(final String transactionalId,
                       final String consumerGroupId,
                       final long producerId,
                       final short producerEpoch,
                       final Map<TopicPartition, CommittedOffset> pendingTxnOffsetCommits) {
            this(transactionalId,
                consumerGroupId,
                producerId,
                producerEpoch,
                pendingTxnOffsetCommits,
                JoinGroupRequest.UNKNOWN_MEMBER_ID,
                JoinGroupRequest.UNKNOWN_GENERATION_ID,
                Optional.empty());
        }

        public Builder(final String transactionalId,
                       final String consumerGroupId,
                       final long producerId,
                       final short producerEpoch,
                       final Map<TopicPartition, CommittedOffset> pendingTxnOffsetCommits,
                       final String memberId,
                       final int generationId,
                       final Optional<String> groupInstanceId) {
            super(ApiKeys.TXN_OFFSET_COMMIT);
            this.data = new TxnOffsetCommitRequestData()
                            .setTransactionalId(transactionalId)
                            .setGroupId(consumerGroupId)
                            .setProducerId(producerId)
                            .setProducerEpoch(producerEpoch)
                            .setTopics(getTopics(pendingTxnOffsetCommits))
                            .setMemberId(memberId)
                            .setGenerationId(generationId)
                            .setGroupInstanceId(groupInstanceId.orElse(null));
        }

        @Override
        public TxnOffsetCommitRequest build(short version) {
            if (version < 3 && groupMetadataSet()) {
                throw new UnsupportedVersionException("Broker doesn't support group metadata commit API on version " + version
                + ", minimum supported request version is 3 which requires brokers to be on version 2.5 or above.");
            }
            return new TxnOffsetCommitRequest(data, version);
        }

        private boolean groupMetadataSet() {
            return !data.memberId().equals(JoinGroupRequest.UNKNOWN_MEMBER_ID) ||
                       data.generationId() != JoinGroupRequest.UNKNOWN_GENERATION_ID ||
                       data.groupInstanceId() != null;
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public TxnOffsetCommitRequest(TxnOffsetCommitRequestData data, short version) {
        super(ApiKeys.TXN_OFFSET_COMMIT, version);
        this.data = data;
    }

    public Map<TopicPartition, CommittedOffset> offsets() {
        List<TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic> topics = data.topics();
        Map<TopicPartition, CommittedOffset> offsetMap = new HashMap<>();
        for (TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic topic : topics) {
            for (TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition partition : topic.partitions()) {
                offsetMap.put(new TopicPartition(topic.name(), partition.partitionIndex()),
                              new CommittedOffset(partition.committedOffset(),
                                                  partition.committedMetadata(),
                                                  RequestUtils.getLeaderEpoch(partition.committedLeaderEpoch()))
                );
            }
        }
        return offsetMap;
    }

    static List<TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic> getTopics(Map<TopicPartition, CommittedOffset> pendingTxnOffsetCommits) {
        Map<String, List<TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition>> topicPartitionMap = new HashMap<>();
        for (Map.Entry<TopicPartition, CommittedOffset> entry : pendingTxnOffsetCommits.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            CommittedOffset offset = entry.getValue();

            List<TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition> partitions =
                topicPartitionMap.getOrDefault(topicPartition.topic(), new ArrayList<>());
            partitions.add(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                               .setPartitionIndex(topicPartition.partition())
                               .setCommittedOffset(offset.offset)
                               .setCommittedLeaderEpoch(offset.leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                               .setCommittedMetadata(offset.metadata)
            );
            topicPartitionMap.put(topicPartition.topic(), partitions);
        }
        return topicPartitionMap.entrySet().stream()
                   .map(entry -> new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                                     .setName(entry.getKey())
                                     .setPartitions(entry.getValue()))
                   .collect(Collectors.toList());
    }

    @Override
    public TxnOffsetCommitRequestData data() {
        return data;
    }

    static List<TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic> getErrorResponseTopics(List<TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic> requestTopics,
                                                                                                 Errors e) {
        List<TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic> responseTopicData = new ArrayList<>();
        for (TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic entry : requestTopics) {
            List<TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition> responsePartitions = new ArrayList<>();
            for (TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition requestPartition : entry.partitions()) {
                responsePartitions.add(new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                                           .setPartitionIndex(requestPartition.partitionIndex())
                                           .setErrorCode(e.code()));
            }
            responseTopicData.add(new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                                      .setName(entry.name())
                                      .setPartitions(responsePartitions)
            );
        }
        return responseTopicData;
    }

    @Override
    public TxnOffsetCommitResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        List<TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic> responseTopicData =
            getErrorResponseTopics(data.topics(), Errors.forException(e));

        return new TxnOffsetCommitResponse(new TxnOffsetCommitResponseData()
                                               .setThrottleTimeMs(throttleTimeMs)
                                               .setTopics(responseTopicData));
    }

    public static TxnOffsetCommitRequest parse(ByteBuffer buffer, short version) {
        return new TxnOffsetCommitRequest(new TxnOffsetCommitRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }

    public static class CommittedOffset {
        public final long offset;
        public final String metadata;
        public final Optional<Integer> leaderEpoch;

        public CommittedOffset(long offset, String metadata, Optional<Integer> leaderEpoch) {
            this.offset = offset;
            this.metadata = metadata;
            this.leaderEpoch = leaderEpoch;
        }

        @Override
        public String toString() {
            return "CommittedOffset(" +
                    "offset=" + offset +
                    ", leaderEpoch=" + leaderEpoch +
                    ", metadata='" + metadata + "')";
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof CommittedOffset)) {
                return false;
            }
            CommittedOffset otherOffset = (CommittedOffset) other;

            return this.offset == otherOffset.offset
                       && this.leaderEpoch.equals(otherOffset.leaderEpoch)
                       && Objects.equals(this.metadata, otherOffset.metadata);
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, leaderEpoch, metadata);
        }
    }
}
