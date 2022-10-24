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
package com.tong.kafka.clients.consumer;

import com.tong.kafka.common.KafkaException;
import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.errors.AuthorizationException;
import com.tong.kafka.common.errors.InterruptException;
import com.tong.kafka.common.errors.RebalanceInProgressException;
import com.tong.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * A callback interface that the user can implement to trigger custom actions when a commit request completes. The callback
 * may be executed in any thread calling {@link Consumer#poll(Duration) poll()}.
 */
public interface OffsetCommitCallback {

    /**
     * A callback method the user can implement to provide asynchronous handling of commit request completion.
     * This method will be called when the commit request sent to the server has been acknowledged.
     *
     * @param offsets A map of the offsets and associated metadata that this callback applies to
     * @param exception The exception thrown during processing of the request, or null if the commit completed successfully
     *
     * @throws CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link KafkaConsumer#subscribe(Collection)},
     *             or if there is an active group with the same groupId which is using group management.
     * @throws RebalanceInProgressException if the commit failed because
     *            it is in the middle of a rebalance. In such cases
     *            commit could be retried after the rebalance is completed with the {@link KafkaConsumer#poll(Duration)} call.
     * @throws WakeupException if {@link KafkaConsumer#wakeup()} is called before or while this
     *             function is called
     * @throws InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the committed offset is invalid).
     */
    void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception);
}
