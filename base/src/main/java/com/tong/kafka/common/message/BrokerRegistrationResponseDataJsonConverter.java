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

public class BrokerRegistrationResponseDataJsonConverter {
    public static BrokerRegistrationResponseData read(JsonNode _node, short _version) {
        BrokerRegistrationResponseData _object = new BrokerRegistrationResponseData();
        JsonNode _throttleTimeMsNode = _node.get("throttleTimeMs");
        if (_throttleTimeMsNode == null) {
            throw new RuntimeException("BrokerRegistrationResponseData: unable to locate field 'throttleTimeMs', which is mandatory in version " + _version);
        } else {
            _object.throttleTimeMs = MessageUtil.jsonNodeToInt(_throttleTimeMsNode, "BrokerRegistrationResponseData");
        }
        JsonNode _errorCodeNode = _node.get("errorCode");
        if (_errorCodeNode == null) {
            throw new RuntimeException("BrokerRegistrationResponseData: unable to locate field 'errorCode', which is mandatory in version " + _version);
        } else {
            _object.errorCode = MessageUtil.jsonNodeToShort(_errorCodeNode, "BrokerRegistrationResponseData");
        }
        JsonNode _brokerEpochNode = _node.get("brokerEpoch");
        if (_brokerEpochNode == null) {
            throw new RuntimeException("BrokerRegistrationResponseData: unable to locate field 'brokerEpoch', which is mandatory in version " + _version);
        } else {
            _object.brokerEpoch = MessageUtil.jsonNodeToLong(_brokerEpochNode, "BrokerRegistrationResponseData");
        }
        return _object;
    }
    public static JsonNode write(BrokerRegistrationResponseData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("throttleTimeMs", new IntNode(_object.throttleTimeMs));
        _node.set("errorCode", new ShortNode(_object.errorCode));
        _node.set("brokerEpoch", new LongNode(_object.brokerEpoch));
        return _node;
    }
    public static JsonNode write(BrokerRegistrationResponseData _object, short _version) {
        return write(_object, _version, true);
    }
}
