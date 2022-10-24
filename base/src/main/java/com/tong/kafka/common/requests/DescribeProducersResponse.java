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

import com.tong.kafka.common.message.DescribeProducersResponseData;
import com.tong.kafka.common.protocol.Errors;
import com.tong.kafka.common.protocol.ApiKeys;
import com.tong.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class DescribeProducersResponse extends AbstractResponse {
    private final DescribeProducersResponseData data;

    public DescribeProducersResponse(DescribeProducersResponseData data) {
        super(ApiKeys.DESCRIBE_PRODUCERS);
        this.data = data;
    }

    @Override
    public DescribeProducersResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (DescribeProducersResponseData.TopicResponse topicResponse : data.topics()) {
            for (DescribeProducersResponseData.PartitionResponse partitionResponse : topicResponse.partitions()) {
                updateErrorCounts(errorCounts, Errors.forCode(partitionResponse.errorCode()));
            }
        }
        return errorCounts;
    }

    public static DescribeProducersResponse parse(ByteBuffer buffer, short version) {
        return new DescribeProducersResponse(new DescribeProducersResponseData(
            new ByteBufferAccessor(buffer), version));
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

}
