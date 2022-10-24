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
package com.tong.kafka.common.errors;

public class GroupAuthorizationException extends AuthorizationException {
    private final String groupId;

    public GroupAuthorizationException(String message, String groupId) {
        super(message);
        this.groupId = groupId;
    }

    public GroupAuthorizationException(String message) {
        this(message, null);
    }

    /**
     * Return the group ID that failed authorization. May be null if it is not known
     * in the context the exception was raised in.
     *
     * @return nullable groupId
     */
    public String groupId() {
        return groupId;
    }

    public static GroupAuthorizationException forGroupId(String groupId) {
        return new GroupAuthorizationException("Not authorized to access group: " + groupId, groupId);
    }

}
