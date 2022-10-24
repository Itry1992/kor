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

// THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.

package com.tong.kafka.common.message;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import com.tong.kafka.common.protocol.MessageUtil;

import java.util.ArrayList;

public class DescribeQuorumRequestDataJsonConverter {
    public static DescribeQuorumRequestData read(JsonNode _node, short _version) {
        DescribeQuorumRequestData _object = new DescribeQuorumRequestData();
        JsonNode _topicsNode = _node.get("topics");
        if (_topicsNode == null) {
            throw new RuntimeException("DescribeQuorumRequestData: unable to locate field 'topics', which is mandatory in version " + _version);
        } else {
            if (!_topicsNode.isArray()) {
                throw new RuntimeException("DescribeQuorumRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<DescribeQuorumRequestData.TopicData> _collection = new ArrayList<DescribeQuorumRequestData.TopicData>(_topicsNode.size());
            _object.topics = _collection;
            for (JsonNode _element : _topicsNode) {
                _collection.add(TopicDataJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(DescribeQuorumRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        ArrayNode _topicsArray = new ArrayNode(JsonNodeFactory.instance);
        for (DescribeQuorumRequestData.TopicData _element : _object.topics) {
            _topicsArray.add(TopicDataJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("topics", _topicsArray);
        return _node;
    }
    public static JsonNode write(DescribeQuorumRequestData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class PartitionDataJsonConverter {
        public static DescribeQuorumRequestData.PartitionData read(JsonNode _node, short _version) {
            DescribeQuorumRequestData.PartitionData _object = new DescribeQuorumRequestData.PartitionData();
            JsonNode _partitionIndexNode = _node.get("partitionIndex");
            if (_partitionIndexNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'partitionIndex', which is mandatory in version " + _version);
            } else {
                _object.partitionIndex = MessageUtil.jsonNodeToInt(_partitionIndexNode, "PartitionData");
            }
            return _object;
        }
        public static JsonNode write(DescribeQuorumRequestData.PartitionData _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("partitionIndex", new IntNode(_object.partitionIndex));
            return _node;
        }
        public static JsonNode write(DescribeQuorumRequestData.PartitionData _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class TopicDataJsonConverter {
        public static DescribeQuorumRequestData.TopicData read(JsonNode _node, short _version) {
            DescribeQuorumRequestData.TopicData _object = new DescribeQuorumRequestData.TopicData();
            JsonNode _topicNameNode = _node.get("topicName");
            if (_topicNameNode == null) {
                throw new RuntimeException("TopicData: unable to locate field 'topicName', which is mandatory in version " + _version);
            } else {
                if (!_topicNameNode.isTextual()) {
                    throw new RuntimeException("TopicData expected a string type, but got " + _node.getNodeType());
                }
                _object.topicName = _topicNameNode.asText();
            }
            JsonNode _partitionsNode = _node.get("partitions");
            if (_partitionsNode == null) {
                throw new RuntimeException("TopicData: unable to locate field 'partitions', which is mandatory in version " + _version);
            } else {
                if (!_partitionsNode.isArray()) {
                    throw new RuntimeException("TopicData expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<DescribeQuorumRequestData.PartitionData> _collection = new ArrayList<DescribeQuorumRequestData.PartitionData>(_partitionsNode.size());
                _object.partitions = _collection;
                for (JsonNode _element : _partitionsNode) {
                    _collection.add(PartitionDataJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(DescribeQuorumRequestData.TopicData _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("topicName", new TextNode(_object.topicName));
            ArrayNode _partitionsArray = new ArrayNode(JsonNodeFactory.instance);
            for (DescribeQuorumRequestData.PartitionData _element : _object.partitions) {
                _partitionsArray.add(PartitionDataJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("partitions", _partitionsArray);
            return _node;
        }
        public static JsonNode write(DescribeQuorumRequestData.TopicData _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
