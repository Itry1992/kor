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
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DescribeClusterRequestDataJsonConverter {
    public static DescribeClusterRequestData read(JsonNode _node, short _version) {
        DescribeClusterRequestData _object = new DescribeClusterRequestData();
        JsonNode _includeClusterAuthorizedOperationsNode = _node.get("includeClusterAuthorizedOperations");
        if (_includeClusterAuthorizedOperationsNode == null) {
            throw new RuntimeException("DescribeClusterRequestData: unable to locate field 'includeClusterAuthorizedOperations', which is mandatory in version " + _version);
        } else {
            if (!_includeClusterAuthorizedOperationsNode.isBoolean()) {
                throw new RuntimeException("DescribeClusterRequestData expected Boolean type, but got " + _node.getNodeType());
            }
            _object.includeClusterAuthorizedOperations = _includeClusterAuthorizedOperationsNode.asBoolean();
        }
        return _object;
    }
    public static JsonNode write(DescribeClusterRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("includeClusterAuthorizedOperations", BooleanNode.valueOf(_object.includeClusterAuthorizedOperations));
        return _node;
    }
    public static JsonNode write(DescribeClusterRequestData _object, short _version) {
        return write(_object, _version, true);
    }
}
