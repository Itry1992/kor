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
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tong.kafka.common.protocol.MessageUtil;

public class ResponseHeaderDataJsonConverter {
    public static ResponseHeaderData read(JsonNode _node, short _version) {
        ResponseHeaderData _object = new ResponseHeaderData();
        JsonNode _correlationIdNode = _node.get("correlationId");
        if (_correlationIdNode == null) {
            throw new RuntimeException("ResponseHeaderData: unable to locate field 'correlationId', which is mandatory in version " + _version);
        } else {
            _object.correlationId = MessageUtil.jsonNodeToInt(_correlationIdNode, "ResponseHeaderData");
        }
        return _object;
    }
    public static JsonNode write(ResponseHeaderData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("correlationId", new IntNode(_object.correlationId));
        return _node;
    }
    public static JsonNode write(ResponseHeaderData _object, short _version) {
        return write(_object, _version, true);
    }
}
