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

public class FetchSnapshotRequestDataJsonConverter {
    public static FetchSnapshotRequestData read(JsonNode _node, short _version) {
        FetchSnapshotRequestData _object = new FetchSnapshotRequestData();
        JsonNode _clusterIdNode = _node.get("clusterId");
        if (_clusterIdNode == null) {
            _object.clusterId = null;
        } else {
            if (_clusterIdNode.isNull()) {
                _object.clusterId = null;
            } else {
                if (!_clusterIdNode.isTextual()) {
                    throw new RuntimeException("FetchSnapshotRequestData expected a string type, but got " + _node.getNodeType());
                }
                _object.clusterId = _clusterIdNode.asText();
            }
        }
        JsonNode _replicaIdNode = _node.get("replicaId");
        if (_replicaIdNode == null) {
            throw new RuntimeException("FetchSnapshotRequestData: unable to locate field 'replicaId', which is mandatory in version " + _version);
        } else {
            _object.replicaId = MessageUtil.jsonNodeToInt(_replicaIdNode, "FetchSnapshotRequestData");
        }
        JsonNode _maxBytesNode = _node.get("maxBytes");
        if (_maxBytesNode == null) {
            throw new RuntimeException("FetchSnapshotRequestData: unable to locate field 'maxBytes', which is mandatory in version " + _version);
        } else {
            _object.maxBytes = MessageUtil.jsonNodeToInt(_maxBytesNode, "FetchSnapshotRequestData");
        }
        JsonNode _topicsNode = _node.get("topics");
        if (_topicsNode == null) {
            throw new RuntimeException("FetchSnapshotRequestData: unable to locate field 'topics', which is mandatory in version " + _version);
        } else {
            if (!_topicsNode.isArray()) {
                throw new RuntimeException("FetchSnapshotRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<FetchSnapshotRequestData.TopicSnapshot> _collection = new ArrayList<FetchSnapshotRequestData.TopicSnapshot>(_topicsNode.size());
            _object.topics = _collection;
            for (JsonNode _element : _topicsNode) {
                _collection.add(TopicSnapshotJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(FetchSnapshotRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        if (_object.clusterId != null) {
            _node.set("clusterId", new TextNode(_object.clusterId));
        }
        _node.set("replicaId", new IntNode(_object.replicaId));
        _node.set("maxBytes", new IntNode(_object.maxBytes));
        ArrayNode _topicsArray = new ArrayNode(JsonNodeFactory.instance);
        for (FetchSnapshotRequestData.TopicSnapshot _element : _object.topics) {
            _topicsArray.add(TopicSnapshotJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("topics", _topicsArray);
        return _node;
    }
    public static JsonNode write(FetchSnapshotRequestData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class PartitionSnapshotJsonConverter {
        public static FetchSnapshotRequestData.PartitionSnapshot read(JsonNode _node, short _version) {
            FetchSnapshotRequestData.PartitionSnapshot _object = new FetchSnapshotRequestData.PartitionSnapshot();
            JsonNode _partitionNode = _node.get("partition");
            if (_partitionNode == null) {
                throw new RuntimeException("PartitionSnapshot: unable to locate field 'partition', which is mandatory in version " + _version);
            } else {
                _object.partition = MessageUtil.jsonNodeToInt(_partitionNode, "PartitionSnapshot");
            }
            JsonNode _currentLeaderEpochNode = _node.get("currentLeaderEpoch");
            if (_currentLeaderEpochNode == null) {
                throw new RuntimeException("PartitionSnapshot: unable to locate field 'currentLeaderEpoch', which is mandatory in version " + _version);
            } else {
                _object.currentLeaderEpoch = MessageUtil.jsonNodeToInt(_currentLeaderEpochNode, "PartitionSnapshot");
            }
            JsonNode _snapshotIdNode = _node.get("snapshotId");
            if (_snapshotIdNode == null) {
                throw new RuntimeException("PartitionSnapshot: unable to locate field 'snapshotId', which is mandatory in version " + _version);
            } else {
                _object.snapshotId = SnapshotIdJsonConverter.read(_snapshotIdNode, _version);
            }
            JsonNode _positionNode = _node.get("position");
            if (_positionNode == null) {
                throw new RuntimeException("PartitionSnapshot: unable to locate field 'position', which is mandatory in version " + _version);
            } else {
                _object.position = MessageUtil.jsonNodeToLong(_positionNode, "PartitionSnapshot");
            }
            return _object;
        }
        public static JsonNode write(FetchSnapshotRequestData.PartitionSnapshot _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("partition", new IntNode(_object.partition));
            _node.set("currentLeaderEpoch", new IntNode(_object.currentLeaderEpoch));
            _node.set("snapshotId", SnapshotIdJsonConverter.write(_object.snapshotId, _version, _serializeRecords));
            _node.set("position", new LongNode(_object.position));
            return _node;
        }
        public static JsonNode write(FetchSnapshotRequestData.PartitionSnapshot _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class SnapshotIdJsonConverter {
        public static FetchSnapshotRequestData.SnapshotId read(JsonNode _node, short _version) {
            FetchSnapshotRequestData.SnapshotId _object = new FetchSnapshotRequestData.SnapshotId();
            JsonNode _endOffsetNode = _node.get("endOffset");
            if (_endOffsetNode == null) {
                throw new RuntimeException("SnapshotId: unable to locate field 'endOffset', which is mandatory in version " + _version);
            } else {
                _object.endOffset = MessageUtil.jsonNodeToLong(_endOffsetNode, "SnapshotId");
            }
            JsonNode _epochNode = _node.get("epoch");
            if (_epochNode == null) {
                throw new RuntimeException("SnapshotId: unable to locate field 'epoch', which is mandatory in version " + _version);
            } else {
                _object.epoch = MessageUtil.jsonNodeToInt(_epochNode, "SnapshotId");
            }
            return _object;
        }
        public static JsonNode write(FetchSnapshotRequestData.SnapshotId _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("endOffset", new LongNode(_object.endOffset));
            _node.set("epoch", new IntNode(_object.epoch));
            return _node;
        }
        public static JsonNode write(FetchSnapshotRequestData.SnapshotId _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class TopicSnapshotJsonConverter {
        public static FetchSnapshotRequestData.TopicSnapshot read(JsonNode _node, short _version) {
            FetchSnapshotRequestData.TopicSnapshot _object = new FetchSnapshotRequestData.TopicSnapshot();
            JsonNode _nameNode = _node.get("name");
            if (_nameNode == null) {
                throw new RuntimeException("TopicSnapshot: unable to locate field 'name', which is mandatory in version " + _version);
            } else {
                if (!_nameNode.isTextual()) {
                    throw new RuntimeException("TopicSnapshot expected a string type, but got " + _node.getNodeType());
                }
                _object.name = _nameNode.asText();
            }
            JsonNode _partitionsNode = _node.get("partitions");
            if (_partitionsNode == null) {
                throw new RuntimeException("TopicSnapshot: unable to locate field 'partitions', which is mandatory in version " + _version);
            } else {
                if (!_partitionsNode.isArray()) {
                    throw new RuntimeException("TopicSnapshot expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<FetchSnapshotRequestData.PartitionSnapshot> _collection = new ArrayList<FetchSnapshotRequestData.PartitionSnapshot>(_partitionsNode.size());
                _object.partitions = _collection;
                for (JsonNode _element : _partitionsNode) {
                    _collection.add(PartitionSnapshotJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(FetchSnapshotRequestData.TopicSnapshot _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("name", new TextNode(_object.name));
            ArrayNode _partitionsArray = new ArrayNode(JsonNodeFactory.instance);
            for (FetchSnapshotRequestData.PartitionSnapshot _element : _object.partitions) {
                _partitionsArray.add(PartitionSnapshotJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("partitions", _partitionsArray);
            return _node;
        }
        public static JsonNode write(FetchSnapshotRequestData.TopicSnapshot _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
