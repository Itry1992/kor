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

public class ListTransactionsRequestDataJsonConverter {
    public static ListTransactionsRequestData read(JsonNode _node, short _version) {
        ListTransactionsRequestData _object = new ListTransactionsRequestData();
        JsonNode _stateFiltersNode = _node.get("stateFilters");
        if (_stateFiltersNode == null) {
            throw new RuntimeException("ListTransactionsRequestData: unable to locate field 'stateFilters', which is mandatory in version " + _version);
        } else {
            if (!_stateFiltersNode.isArray()) {
                throw new RuntimeException("ListTransactionsRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<String> _collection = new ArrayList<String>(_stateFiltersNode.size());
            _object.stateFilters = _collection;
            for (JsonNode _element : _stateFiltersNode) {
                if (!_element.isTextual()) {
                    throw new RuntimeException("ListTransactionsRequestData element expected a string type, but got " + _node.getNodeType());
                }
                _collection.add(_element.asText());
            }
        }
        JsonNode _producerIdFiltersNode = _node.get("producerIdFilters");
        if (_producerIdFiltersNode == null) {
            throw new RuntimeException("ListTransactionsRequestData: unable to locate field 'producerIdFilters', which is mandatory in version " + _version);
        } else {
            if (!_producerIdFiltersNode.isArray()) {
                throw new RuntimeException("ListTransactionsRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<Long> _collection = new ArrayList<Long>(_producerIdFiltersNode.size());
            _object.producerIdFilters = _collection;
            for (JsonNode _element : _producerIdFiltersNode) {
                _collection.add(MessageUtil.jsonNodeToLong(_element, "ListTransactionsRequestData element"));
            }
        }
        return _object;
    }
    public static JsonNode write(ListTransactionsRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        ArrayNode _stateFiltersArray = new ArrayNode(JsonNodeFactory.instance);
        for (String _element : _object.stateFilters) {
            _stateFiltersArray.add(new TextNode(_element));
        }
        _node.set("stateFilters", _stateFiltersArray);
        ArrayNode _producerIdFiltersArray = new ArrayNode(JsonNodeFactory.instance);
        for (Long _element : _object.producerIdFilters) {
            _producerIdFiltersArray.add(new LongNode(_element));
        }
        _node.set("producerIdFilters", _producerIdFiltersArray);
        return _node;
    }
    public static JsonNode write(ListTransactionsRequestData _object, short _version) {
        return write(_object, _version, true);
    }
}
