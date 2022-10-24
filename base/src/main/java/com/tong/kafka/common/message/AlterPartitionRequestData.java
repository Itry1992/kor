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

import com.tong.kafka.common.Uuid;
import com.tong.kafka.common.errors.UnsupportedVersionException;
import com.tong.kafka.common.protocol.*;
import com.tong.kafka.common.protocol.Readable;
import com.tong.kafka.common.protocol.types.*;


import com.tong.kafka.common.utils.ByteUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.tong.kafka.common.protocol.types.Field.TaggedFieldsSection;


public class AlterPartitionRequestData implements ApiMessage {
    int brokerId;
    long brokerEpoch;
    List<TopicData> topics;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("broker_id", Type.INT32, "The ID of the requesting broker"),
            new Field("broker_epoch", Type.INT64, "The epoch of the requesting broker"),
            new Field("topics", new CompactArrayOf(TopicData.SCHEMA_0), ""),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("broker_id", Type.INT32, "The ID of the requesting broker"),
            new Field("broker_epoch", Type.INT64, "The epoch of the requesting broker"),
            new Field("topics", new CompactArrayOf(TopicData.SCHEMA_1), ""),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema SCHEMA_2 =
        new Schema(
            new Field("broker_id", Type.INT32, "The ID of the requesting broker"),
            new Field("broker_epoch", Type.INT64, "The epoch of the requesting broker"),
            new Field("topics", new CompactArrayOf(TopicData.SCHEMA_2), ""),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1,
        SCHEMA_2
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 2;
    
    public AlterPartitionRequestData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public AlterPartitionRequestData() {
        this.brokerId = 0;
        this.brokerEpoch = -1L;
        this.topics = new ArrayList<TopicData>(0);
    }
    
    @Override
    public short apiKey() {
        return 56;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 2;
    }
    
    @Override
    public void read(Readable _readable, short _version) {
        this.brokerId = _readable.readInt();
        this.brokerEpoch = _readable.readLong();
        {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field topics was serialized as null");
            } else {
                if (arrayLength > _readable.remaining()) {
                    throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                }
                ArrayList<TopicData> newCollection = new ArrayList<>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new TopicData(_readable, _version));
                }
                this.topics = newCollection;
            }
        }
        this._unknownTaggedFields = null;
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; _i++) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                    break;
            }
        }
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _writable.writeInt(brokerId);
        _writable.writeLong(brokerEpoch);
        _writable.writeUnsignedVarint(topics.size() + 1);
        for (TopicData topicsElement : topics) {
            topicsElement.write(_writable, _cache, _version);
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        _writable.writeUnsignedVarint(_numTaggedFields);
        _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _size.addBytes(4);
        _size.addBytes(8);
        {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(topics.size() + 1));
            for (TopicData topicsElement : topics) {
                topicsElement.addSize(_size, _cache, _version);
            }
        }
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                _size.addBytes(_field.size());
            }
        }
        _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AlterPartitionRequestData)) return false;
        AlterPartitionRequestData other = (AlterPartitionRequestData) obj;
        if (brokerId != other.brokerId) return false;
        if (brokerEpoch != other.brokerEpoch) return false;
        if (this.topics == null) {
            if (other.topics != null) return false;
        } else {
            if (!this.topics.equals(other.topics)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + brokerId;
        hashCode = 31 * hashCode + ((int) (brokerEpoch >> 32) ^ (int) brokerEpoch);
        hashCode = 31 * hashCode + (topics == null ? 0 : topics.hashCode());
        return hashCode;
    }
    
    @Override
    public AlterPartitionRequestData duplicate() {
        AlterPartitionRequestData _duplicate = new AlterPartitionRequestData();
        _duplicate.brokerId = brokerId;
        _duplicate.brokerEpoch = brokerEpoch;
        ArrayList<TopicData> newTopics = new ArrayList<TopicData>(topics.size());
        for (TopicData _element : topics) {
            newTopics.add(_element.duplicate());
        }
        _duplicate.topics = newTopics;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "AlterPartitionRequestData("
            + "brokerId=" + brokerId
            + ", brokerEpoch=" + brokerEpoch
            + ", topics=" + MessageUtil.deepToString(topics.iterator())
            + ")";
    }
    
    public int brokerId() {
        return this.brokerId;
    }
    
    public long brokerEpoch() {
        return this.brokerEpoch;
    }
    
    public List<TopicData> topics() {
        return this.topics;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public AlterPartitionRequestData setBrokerId(int v) {
        this.brokerId = v;
        return this;
    }
    
    public AlterPartitionRequestData setBrokerEpoch(long v) {
        this.brokerEpoch = v;
        return this;
    }
    
    public AlterPartitionRequestData setTopics(List<TopicData> v) {
        this.topics = v;
        return this;
    }
    
    public static class TopicData implements Message {
        String topicName;
        Uuid topicId;
        List<PartitionData> partitions;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("topic_name", Type.COMPACT_STRING, "The name of the topic to alter ISRs for"),
                new Field("partitions", new CompactArrayOf(PartitionData.SCHEMA_0), ""),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("topic_name", Type.COMPACT_STRING, "The name of the topic to alter ISRs for"),
                new Field("partitions", new CompactArrayOf(PartitionData.SCHEMA_1), ""),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_2 =
            new Schema(
                new Field("topic_id", Type.UUID, "The ID of the topic to alter ISRs for"),
                new Field("partitions", new CompactArrayOf(PartitionData.SCHEMA_1), ""),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1,
            SCHEMA_2
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 2;
        
        public TopicData(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public TopicData() {
            this.topicName = "";
            this.topicId = Uuid.ZERO_UUID;
            this.partitions = new ArrayList<PartitionData>(0);
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 2;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of TopicData");
            }
            if (_version <= 1) {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    throw new RuntimeException("non-nullable field topicName was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field topicName had invalid length " + length);
                } else {
                    this.topicName = _readable.readString(length);
                }
            } else {
                this.topicName = "";
            }
            if (_version >= 2) {
                this.topicId = _readable.readUuid();
            } else {
                this.topicId = Uuid.ZERO_UUID;
            }
            {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field partitions was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    ArrayList<PartitionData> newCollection = new ArrayList<>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new PartitionData(_readable, _version));
                    }
                    this.partitions = newCollection;
                }
            }
            this._unknownTaggedFields = null;
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version <= 1) {
                {
                    byte[] _stringBytes = _cache.getSerializedValue(topicName);
                    _writable.writeUnsignedVarint(_stringBytes.length + 1);
                    _writable.writeByteArray(_stringBytes);
                }
            }
            if (_version >= 2) {
                _writable.writeUuid(topicId);
            }
            _writable.writeUnsignedVarint(partitions.size() + 1);
            for (PartitionData partitionsElement : partitions) {
                partitionsElement.write(_writable, _cache, _version);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of TopicData");
            }
            if (_version <= 1) {
                {
                    byte[] _stringBytes = topicName.getBytes(StandardCharsets.UTF_8);
                    if (_stringBytes.length > 0x7fff) {
                        throw new RuntimeException("'topicName' field is too long to be serialized");
                    }
                    _cache.cacheSerializedValue(topicName, _stringBytes);
                    _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
                }
            }
            if (_version >= 2) {
                _size.addBytes(16);
            }
            {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(partitions.size() + 1));
                for (PartitionData partitionsElement : partitions) {
                    partitionsElement.addSize(_size, _cache, _version);
                }
            }
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TopicData)) return false;
            TopicData other = (TopicData) obj;
            if (this.topicName == null) {
                if (other.topicName != null) return false;
            } else {
                if (!this.topicName.equals(other.topicName)) return false;
            }
            if (!this.topicId.equals(other.topicId)) return false;
            if (this.partitions == null) {
                if (other.partitions != null) return false;
            } else {
                if (!this.partitions.equals(other.partitions)) return false;
            }
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (topicName == null ? 0 : topicName.hashCode());
            hashCode = 31 * hashCode + topicId.hashCode();
            hashCode = 31 * hashCode + (partitions == null ? 0 : partitions.hashCode());
            return hashCode;
        }
        
        @Override
        public TopicData duplicate() {
            TopicData _duplicate = new TopicData();
            _duplicate.topicName = topicName;
            _duplicate.topicId = topicId;
            ArrayList<PartitionData> newPartitions = new ArrayList<PartitionData>(partitions.size());
            for (PartitionData _element : partitions) {
                newPartitions.add(_element.duplicate());
            }
            _duplicate.partitions = newPartitions;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "TopicData("
                + "topicName=" + ((topicName == null) ? "null" : "'" + topicName.toString() + "'")
                + ", topicId=" + topicId.toString()
                + ", partitions=" + MessageUtil.deepToString(partitions.iterator())
                + ")";
        }
        
        public String topicName() {
            return this.topicName;
        }
        
        public Uuid topicId() {
            return this.topicId;
        }
        
        public List<PartitionData> partitions() {
            return this.partitions;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public TopicData setTopicName(String v) {
            this.topicName = v;
            return this;
        }
        
        public TopicData setTopicId(Uuid v) {
            this.topicId = v;
            return this;
        }
        
        public TopicData setPartitions(List<PartitionData> v) {
            this.partitions = v;
            return this;
        }
    }
    
    public static class PartitionData implements Message {
        int partitionIndex;
        int leaderEpoch;
        List<Integer> newIsr;
        byte leaderRecoveryState;
        int partitionEpoch;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index"),
                new Field("leader_epoch", Type.INT32, "The leader epoch of this partition"),
                new Field("new_isr", new CompactArrayOf(Type.INT32), "The ISR for this partition"),
                new Field("partition_epoch", Type.INT32, "The expected epoch of the partition which is being updated. For legacy cluster this is the ZkVersion in the LeaderAndIsr request."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index"),
                new Field("leader_epoch", Type.INT32, "The leader epoch of this partition"),
                new Field("new_isr", new CompactArrayOf(Type.INT32), "The ISR for this partition"),
                new Field("leader_recovery_state", Type.INT8, "1 if the partition is recovering from an unclean leader election; 0 otherwise."),
                new Field("partition_epoch", Type.INT32, "The expected epoch of the partition which is being updated. For legacy cluster this is the ZkVersion in the LeaderAndIsr request."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_2 = SCHEMA_1;
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1,
            SCHEMA_2
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 2;
        
        public PartitionData(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public PartitionData() {
            this.partitionIndex = 0;
            this.leaderEpoch = 0;
            this.newIsr = new ArrayList<Integer>(0);
            this.leaderRecoveryState = (byte) 0;
            this.partitionEpoch = 0;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 2;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of PartitionData");
            }
            this.partitionIndex = _readable.readInt();
            this.leaderEpoch = _readable.readInt();
            {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field newIsr was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    ArrayList<Integer> newCollection = new ArrayList<>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(_readable.readInt());
                    }
                    this.newIsr = newCollection;
                }
            }
            if (_version >= 1) {
                this.leaderRecoveryState = _readable.readByte();
            } else {
                this.leaderRecoveryState = (byte) 0;
            }
            this.partitionEpoch = _readable.readInt();
            this._unknownTaggedFields = null;
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            _writable.writeInt(partitionIndex);
            _writable.writeInt(leaderEpoch);
            _writable.writeUnsignedVarint(newIsr.size() + 1);
            for (Integer newIsrElement : newIsr) {
                _writable.writeInt(newIsrElement);
            }
            if (_version >= 1) {
                _writable.writeByte(leaderRecoveryState);
            } else {
                if (this.leaderRecoveryState != (byte) 0) {
                    throw new UnsupportedVersionException("Attempted to write a non-default leaderRecoveryState at version " + _version);
                }
            }
            _writable.writeInt(partitionEpoch);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of PartitionData");
            }
            _size.addBytes(4);
            _size.addBytes(4);
            {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(newIsr.size() + 1));
                _size.addBytes(newIsr.size() * 4);
            }
            if (_version >= 1) {
                _size.addBytes(1);
            }
            _size.addBytes(4);
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PartitionData)) return false;
            PartitionData other = (PartitionData) obj;
            if (partitionIndex != other.partitionIndex) return false;
            if (leaderEpoch != other.leaderEpoch) return false;
            if (this.newIsr == null) {
                if (other.newIsr != null) return false;
            } else {
                if (!this.newIsr.equals(other.newIsr)) return false;
            }
            if (leaderRecoveryState != other.leaderRecoveryState) return false;
            if (partitionEpoch != other.partitionEpoch) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + partitionIndex;
            hashCode = 31 * hashCode + leaderEpoch;
            hashCode = 31 * hashCode + (newIsr == null ? 0 : newIsr.hashCode());
            hashCode = 31 * hashCode + leaderRecoveryState;
            hashCode = 31 * hashCode + partitionEpoch;
            return hashCode;
        }
        
        @Override
        public PartitionData duplicate() {
            PartitionData _duplicate = new PartitionData();
            _duplicate.partitionIndex = partitionIndex;
            _duplicate.leaderEpoch = leaderEpoch;
            ArrayList<Integer> newNewIsr = new ArrayList<Integer>(newIsr.size());
            for (Integer _element : newIsr) {
                newNewIsr.add(_element);
            }
            _duplicate.newIsr = newNewIsr;
            _duplicate.leaderRecoveryState = leaderRecoveryState;
            _duplicate.partitionEpoch = partitionEpoch;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "PartitionData("
                + "partitionIndex=" + partitionIndex
                + ", leaderEpoch=" + leaderEpoch
                + ", newIsr=" + MessageUtil.deepToString(newIsr.iterator())
                + ", leaderRecoveryState=" + leaderRecoveryState
                + ", partitionEpoch=" + partitionEpoch
                + ")";
        }
        
        public int partitionIndex() {
            return this.partitionIndex;
        }
        
        public int leaderEpoch() {
            return this.leaderEpoch;
        }
        
        public List<Integer> newIsr() {
            return this.newIsr;
        }
        
        public byte leaderRecoveryState() {
            return this.leaderRecoveryState;
        }
        
        public int partitionEpoch() {
            return this.partitionEpoch;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public PartitionData setPartitionIndex(int v) {
            this.partitionIndex = v;
            return this;
        }
        
        public PartitionData setLeaderEpoch(int v) {
            this.leaderEpoch = v;
            return this;
        }
        
        public PartitionData setNewIsr(List<Integer> v) {
            this.newIsr = v;
            return this;
        }
        
        public PartitionData setLeaderRecoveryState(byte v) {
            this.leaderRecoveryState = v;
            return this;
        }
        
        public PartitionData setPartitionEpoch(int v) {
            this.partitionEpoch = v;
            return this;
        }
    }
}
