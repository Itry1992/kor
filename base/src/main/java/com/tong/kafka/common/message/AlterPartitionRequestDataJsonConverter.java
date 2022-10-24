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
import com.tong.kafka.common.Uuid;
import com.tong.kafka.common.errors.UnsupportedVersionException;
import com.tong.kafka.common.protocol.MessageUtil;

import java.util.ArrayList;

public class AlterPartitionRequestDataJsonConverter {
    public static AlterPartitionRequestData read(JsonNode _node, short _version) {
        AlterPartitionRequestData _object = new AlterPartitionRequestData();
        JsonNode _brokerIdNode = _node.get("brokerId");
        if (_brokerIdNode == null) {
            throw new RuntimeException("AlterPartitionRequestData: unable to locate field 'brokerId', which is mandatory in version " + _version);
        } else {
            _object.brokerId = MessageUtil.jsonNodeToInt(_brokerIdNode, "AlterPartitionRequestData");
        }
        JsonNode _brokerEpochNode = _node.get("brokerEpoch");
        if (_brokerEpochNode == null) {
            throw new RuntimeException("AlterPartitionRequestData: unable to locate field 'brokerEpoch', which is mandatory in version " + _version);
        } else {
            _object.brokerEpoch = MessageUtil.jsonNodeToLong(_brokerEpochNode, "AlterPartitionRequestData");
        }
        JsonNode _topicsNode = _node.get("topics");
        if (_topicsNode == null) {
            throw new RuntimeException("AlterPartitionRequestData: unable to locate field 'topics', which is mandatory in version " + _version);
        } else {
            if (!_topicsNode.isArray()) {
                throw new RuntimeException("AlterPartitionRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<AlterPartitionRequestData.TopicData> _collection = new ArrayList<AlterPartitionRequestData.TopicData>(_topicsNode.size());
            _object.topics = _collection;
            for (JsonNode _element : _topicsNode) {
                _collection.add(TopicDataJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(AlterPartitionRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("brokerId", new IntNode(_object.brokerId));
        _node.set("brokerEpoch", new LongNode(_object.brokerEpoch));
        ArrayNode _topicsArray = new ArrayNode(JsonNodeFactory.instance);
        for (AlterPartitionRequestData.TopicData _element : _object.topics) {
            _topicsArray.add(TopicDataJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("topics", _topicsArray);
        return _node;
    }
    public static JsonNode write(AlterPartitionRequestData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class PartitionDataJsonConverter {
        public static AlterPartitionRequestData.PartitionData read(JsonNode _node, short _version) {
            AlterPartitionRequestData.PartitionData _object = new AlterPartitionRequestData.PartitionData();
            JsonNode _partitionIndexNode = _node.get("partitionIndex");
            if (_partitionIndexNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'partitionIndex', which is mandatory in version " + _version);
            } else {
                _object.partitionIndex = MessageUtil.jsonNodeToInt(_partitionIndexNode, "PartitionData");
            }
            JsonNode _leaderEpochNode = _node.get("leaderEpoch");
            if (_leaderEpochNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'leaderEpoch', which is mandatory in version " + _version);
            } else {
                _object.leaderEpoch = MessageUtil.jsonNodeToInt(_leaderEpochNode, "PartitionData");
            }
            JsonNode _newIsrNode = _node.get("newIsr");
            if (_newIsrNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'newIsr', which is mandatory in version " + _version);
            } else {
                if (!_newIsrNode.isArray()) {
                    throw new RuntimeException("PartitionData expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<Integer> _collection = new ArrayList<Integer>(_newIsrNode.size());
                _object.newIsr = _collection;
                for (JsonNode _element : _newIsrNode) {
                    _collection.add(MessageUtil.jsonNodeToInt(_element, "PartitionData element"));
                }
            }
            JsonNode _leaderRecoveryStateNode = _node.get("leaderRecoveryState");
            if (_leaderRecoveryStateNode == null) {
                if (_version >= 1) {
                    throw new RuntimeException("PartitionData: unable to locate field 'leaderRecoveryState', which is mandatory in version " + _version);
                } else {
                    _object.leaderRecoveryState = (byte) 0;
                }
            } else {
                _object.leaderRecoveryState = MessageUtil.jsonNodeToByte(_leaderRecoveryStateNode, "PartitionData");
            }
            JsonNode _partitionEpochNode = _node.get("partitionEpoch");
            if (_partitionEpochNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'partitionEpoch', which is mandatory in version " + _version);
            } else {
                _object.partitionEpoch = MessageUtil.jsonNodeToInt(_partitionEpochNode, "PartitionData");
            }
            return _object;
        }
        public static JsonNode write(AlterPartitionRequestData.PartitionData _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("partitionIndex", new IntNode(_object.partitionIndex));
            _node.set("leaderEpoch", new IntNode(_object.leaderEpoch));
            ArrayNode _newIsrArray = new ArrayNode(JsonNodeFactory.instance);
            for (Integer _element : _object.newIsr) {
                _newIsrArray.add(new IntNode(_element));
            }
            _node.set("newIsr", _newIsrArray);
            if (_version >= 1) {
                _node.set("leaderRecoveryState", new ShortNode(_object.leaderRecoveryState));
            } else {
                if (_object.leaderRecoveryState != (byte) 0) {
                    throw new UnsupportedVersionException("Attempted to write a non-default leaderRecoveryState at version " + _version);
                }
            }
            _node.set("partitionEpoch", new IntNode(_object.partitionEpoch));
            return _node;
        }
        public static JsonNode write(AlterPartitionRequestData.PartitionData _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class TopicDataJsonConverter {
        public static AlterPartitionRequestData.TopicData read(JsonNode _node, short _version) {
            AlterPartitionRequestData.TopicData _object = new AlterPartitionRequestData.TopicData();
            JsonNode _topicNameNode = _node.get("topicName");
            if (_topicNameNode == null) {
                if (_version <= 1) {
                    throw new RuntimeException("TopicData: unable to locate field 'topicName', which is mandatory in version " + _version);
                } else {
                    _object.topicName = "";
                }
            } else {
                if (!_topicNameNode.isTextual()) {
                    throw new RuntimeException("TopicData expected a string type, but got " + _node.getNodeType());
                }
                _object.topicName = _topicNameNode.asText();
            }
            JsonNode _topicIdNode = _node.get("topicId");
            if (_topicIdNode == null) {
                if (_version >= 2) {
                    throw new RuntimeException("TopicData: unable to locate field 'topicId', which is mandatory in version " + _version);
                } else {
                    _object.topicId = Uuid.ZERO_UUID;
                }
            } else {
                if (!_topicIdNode.isTextual()) {
                    throw new RuntimeException("TopicData expected a JSON string type, but got " + _node.getNodeType());
                }
                _object.topicId = Uuid.fromString(_topicIdNode.asText());
            }
            JsonNode _partitionsNode = _node.get("partitions");
            if (_partitionsNode == null) {
                throw new RuntimeException("TopicData: unable to locate field 'partitions', which is mandatory in version " + _version);
            } else {
                if (!_partitionsNode.isArray()) {
                    throw new RuntimeException("TopicData expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<AlterPartitionRequestData.PartitionData> _collection = new ArrayList<AlterPartitionRequestData.PartitionData>(_partitionsNode.size());
                _object.partitions = _collection;
                for (JsonNode _element : _partitionsNode) {
                    _collection.add(PartitionDataJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(AlterPartitionRequestData.TopicData _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            if (_version <= 1) {
                _node.set("topicName", new TextNode(_object.topicName));
            }
            if (_version >= 2) {
                _node.set("topicId", new TextNode(_object.topicId.toString()));
            }
            ArrayNode _partitionsArray = new ArrayNode(JsonNodeFactory.instance);
            for (AlterPartitionRequestData.PartitionData _element : _object.partitions) {
                _partitionsArray.add(PartitionDataJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("partitions", _partitionsArray);
            return _node;
        }
        public static JsonNode write(AlterPartitionRequestData.TopicData _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
