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

public class OffsetCommitResponseDataJsonConverter {
    public static OffsetCommitResponseData read(JsonNode _node, short _version) {
        OffsetCommitResponseData _object = new OffsetCommitResponseData();
        JsonNode _throttleTimeMsNode = _node.get("throttleTimeMs");
        if (_throttleTimeMsNode == null) {
            if (_version >= 3) {
                throw new RuntimeException("OffsetCommitResponseData: unable to locate field 'throttleTimeMs', which is mandatory in version " + _version);
            } else {
                _object.throttleTimeMs = 0;
            }
        } else {
            _object.throttleTimeMs = MessageUtil.jsonNodeToInt(_throttleTimeMsNode, "OffsetCommitResponseData");
        }
        JsonNode _topicsNode = _node.get("topics");
        if (_topicsNode == null) {
            throw new RuntimeException("OffsetCommitResponseData: unable to locate field 'topics', which is mandatory in version " + _version);
        } else {
            if (!_topicsNode.isArray()) {
                throw new RuntimeException("OffsetCommitResponseData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<OffsetCommitResponseData.OffsetCommitResponseTopic> _collection = new ArrayList<OffsetCommitResponseData.OffsetCommitResponseTopic>(_topicsNode.size());
            _object.topics = _collection;
            for (JsonNode _element : _topicsNode) {
                _collection.add(OffsetCommitResponseTopicJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(OffsetCommitResponseData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        if (_version >= 3) {
            _node.set("throttleTimeMs", new IntNode(_object.throttleTimeMs));
        }
        ArrayNode _topicsArray = new ArrayNode(JsonNodeFactory.instance);
        for (OffsetCommitResponseData.OffsetCommitResponseTopic _element : _object.topics) {
            _topicsArray.add(OffsetCommitResponseTopicJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("topics", _topicsArray);
        return _node;
    }
    public static JsonNode write(OffsetCommitResponseData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class OffsetCommitResponsePartitionJsonConverter {
        public static OffsetCommitResponseData.OffsetCommitResponsePartition read(JsonNode _node, short _version) {
            OffsetCommitResponseData.OffsetCommitResponsePartition _object = new OffsetCommitResponseData.OffsetCommitResponsePartition();
            JsonNode _partitionIndexNode = _node.get("partitionIndex");
            if (_partitionIndexNode == null) {
                throw new RuntimeException("OffsetCommitResponsePartition: unable to locate field 'partitionIndex', which is mandatory in version " + _version);
            } else {
                _object.partitionIndex = MessageUtil.jsonNodeToInt(_partitionIndexNode, "OffsetCommitResponsePartition");
            }
            JsonNode _errorCodeNode = _node.get("errorCode");
            if (_errorCodeNode == null) {
                throw new RuntimeException("OffsetCommitResponsePartition: unable to locate field 'errorCode', which is mandatory in version " + _version);
            } else {
                _object.errorCode = MessageUtil.jsonNodeToShort(_errorCodeNode, "OffsetCommitResponsePartition");
            }
            return _object;
        }
        public static JsonNode write(OffsetCommitResponseData.OffsetCommitResponsePartition _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("partitionIndex", new IntNode(_object.partitionIndex));
            _node.set("errorCode", new ShortNode(_object.errorCode));
            return _node;
        }
        public static JsonNode write(OffsetCommitResponseData.OffsetCommitResponsePartition _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class OffsetCommitResponseTopicJsonConverter {
        public static OffsetCommitResponseData.OffsetCommitResponseTopic read(JsonNode _node, short _version) {
            OffsetCommitResponseData.OffsetCommitResponseTopic _object = new OffsetCommitResponseData.OffsetCommitResponseTopic();
            JsonNode _nameNode = _node.get("name");
            if (_nameNode == null) {
                throw new RuntimeException("OffsetCommitResponseTopic: unable to locate field 'name', which is mandatory in version " + _version);
            } else {
                if (!_nameNode.isTextual()) {
                    throw new RuntimeException("OffsetCommitResponseTopic expected a string type, but got " + _node.getNodeType());
                }
                _object.name = _nameNode.asText();
            }
            JsonNode _partitionsNode = _node.get("partitions");
            if (_partitionsNode == null) {
                throw new RuntimeException("OffsetCommitResponseTopic: unable to locate field 'partitions', which is mandatory in version " + _version);
            } else {
                if (!_partitionsNode.isArray()) {
                    throw new RuntimeException("OffsetCommitResponseTopic expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<OffsetCommitResponseData.OffsetCommitResponsePartition> _collection = new ArrayList<OffsetCommitResponseData.OffsetCommitResponsePartition>(_partitionsNode.size());
                _object.partitions = _collection;
                for (JsonNode _element : _partitionsNode) {
                    _collection.add(OffsetCommitResponsePartitionJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(OffsetCommitResponseData.OffsetCommitResponseTopic _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("name", new TextNode(_object.name));
            ArrayNode _partitionsArray = new ArrayNode(JsonNodeFactory.instance);
            for (OffsetCommitResponseData.OffsetCommitResponsePartition _element : _object.partitions) {
                _partitionsArray.add(OffsetCommitResponsePartitionJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("partitions", _partitionsArray);
            return _node;
        }
        public static JsonNode write(OffsetCommitResponseData.OffsetCommitResponseTopic _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
