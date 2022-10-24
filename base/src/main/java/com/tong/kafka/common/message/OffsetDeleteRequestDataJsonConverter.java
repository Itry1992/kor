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

public class OffsetDeleteRequestDataJsonConverter {
    public static OffsetDeleteRequestData read(JsonNode _node, short _version) {
        OffsetDeleteRequestData _object = new OffsetDeleteRequestData();
        JsonNode _groupIdNode = _node.get("groupId");
        if (_groupIdNode == null) {
            throw new RuntimeException("OffsetDeleteRequestData: unable to locate field 'groupId', which is mandatory in version " + _version);
        } else {
            if (!_groupIdNode.isTextual()) {
                throw new RuntimeException("OffsetDeleteRequestData expected a string type, but got " + _node.getNodeType());
            }
            _object.groupId = _groupIdNode.asText();
        }
        JsonNode _topicsNode = _node.get("topics");
        if (_topicsNode == null) {
            throw new RuntimeException("OffsetDeleteRequestData: unable to locate field 'topics', which is mandatory in version " + _version);
        } else {
            if (!_topicsNode.isArray()) {
                throw new RuntimeException("OffsetDeleteRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection _collection = new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection(_topicsNode.size());
            _object.topics = _collection;
            for (JsonNode _element : _topicsNode) {
                _collection.add(OffsetDeleteRequestTopicJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(OffsetDeleteRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("groupId", new TextNode(_object.groupId));
        ArrayNode _topicsArray = new ArrayNode(JsonNodeFactory.instance);
        for (OffsetDeleteRequestData.OffsetDeleteRequestTopic _element : _object.topics) {
            _topicsArray.add(OffsetDeleteRequestTopicJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("topics", _topicsArray);
        return _node;
    }
    public static JsonNode write(OffsetDeleteRequestData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class OffsetDeleteRequestPartitionJsonConverter {
        public static OffsetDeleteRequestData.OffsetDeleteRequestPartition read(JsonNode _node, short _version) {
            OffsetDeleteRequestData.OffsetDeleteRequestPartition _object = new OffsetDeleteRequestData.OffsetDeleteRequestPartition();
            JsonNode _partitionIndexNode = _node.get("partitionIndex");
            if (_partitionIndexNode == null) {
                throw new RuntimeException("OffsetDeleteRequestPartition: unable to locate field 'partitionIndex', which is mandatory in version " + _version);
            } else {
                _object.partitionIndex = MessageUtil.jsonNodeToInt(_partitionIndexNode, "OffsetDeleteRequestPartition");
            }
            return _object;
        }
        public static JsonNode write(OffsetDeleteRequestData.OffsetDeleteRequestPartition _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("partitionIndex", new IntNode(_object.partitionIndex));
            return _node;
        }
        public static JsonNode write(OffsetDeleteRequestData.OffsetDeleteRequestPartition _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class OffsetDeleteRequestTopicJsonConverter {
        public static OffsetDeleteRequestData.OffsetDeleteRequestTopic read(JsonNode _node, short _version) {
            OffsetDeleteRequestData.OffsetDeleteRequestTopic _object = new OffsetDeleteRequestData.OffsetDeleteRequestTopic();
            JsonNode _nameNode = _node.get("name");
            if (_nameNode == null) {
                throw new RuntimeException("OffsetDeleteRequestTopic: unable to locate field 'name', which is mandatory in version " + _version);
            } else {
                if (!_nameNode.isTextual()) {
                    throw new RuntimeException("OffsetDeleteRequestTopic expected a string type, but got " + _node.getNodeType());
                }
                _object.name = _nameNode.asText();
            }
            JsonNode _partitionsNode = _node.get("partitions");
            if (_partitionsNode == null) {
                throw new RuntimeException("OffsetDeleteRequestTopic: unable to locate field 'partitions', which is mandatory in version " + _version);
            } else {
                if (!_partitionsNode.isArray()) {
                    throw new RuntimeException("OffsetDeleteRequestTopic expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<OffsetDeleteRequestData.OffsetDeleteRequestPartition> _collection = new ArrayList<OffsetDeleteRequestData.OffsetDeleteRequestPartition>(_partitionsNode.size());
                _object.partitions = _collection;
                for (JsonNode _element : _partitionsNode) {
                    _collection.add(OffsetDeleteRequestPartitionJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(OffsetDeleteRequestData.OffsetDeleteRequestTopic _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("name", new TextNode(_object.name));
            ArrayNode _partitionsArray = new ArrayNode(JsonNodeFactory.instance);
            for (OffsetDeleteRequestData.OffsetDeleteRequestPartition _element : _object.partitions) {
                _partitionsArray.add(OffsetDeleteRequestPartitionJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("partitions", _partitionsArray);
            return _node;
        }
        public static JsonNode write(OffsetDeleteRequestData.OffsetDeleteRequestTopic _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
