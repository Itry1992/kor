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
package com.tong.kafka.common.requests;

import com.tong.kafka.common.acl.AclPermissionType;
import com.tong.kafka.common.message.DeleteAclsResponseData;
import com.tong.kafka.common.protocol.ApiKeys;
import com.tong.kafka.common.protocol.ByteBufferAccessor;
import com.tong.kafka.common.protocol.Errors;
import com.tong.kafka.common.acl.AccessControlEntry;
import com.tong.kafka.common.acl.AclBinding;
import com.tong.kafka.common.acl.AclOperation;
import com.tong.kafka.common.errors.UnsupportedVersionException;
import com.tong.kafka.common.resource.PatternType;
import com.tong.kafka.common.resource.ResourcePattern;
import com.tong.kafka.common.resource.ResourceType;
import com.tong.kafka.server.authorizer.AclDeleteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeleteAclsResponse extends AbstractResponse {
    public static final Logger log = LoggerFactory.getLogger(DeleteAclsResponse.class);

    private final DeleteAclsResponseData data;

    public DeleteAclsResponse(DeleteAclsResponseData data, short version) {
        super(ApiKeys.DELETE_ACLS);
        this.data = data;
        validate(version);
    }

    @Override
    public DeleteAclsResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public List<DeleteAclsResponseData.DeleteAclsFilterResult> filterResults() {
        return data.filterResults();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(filterResults().stream().map(r -> Errors.forCode(r.errorCode())));
    }

    public static DeleteAclsResponse parse(ByteBuffer buffer, short version) {
        return new DeleteAclsResponse(new DeleteAclsResponseData(new ByteBufferAccessor(buffer), version), version);
    }

    public String toString() {
        return data.toString();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }

    private void validate(short version) {
        if (version == 0) {
            final boolean unsupported = filterResults().stream()
                .flatMap(r -> r.matchingAcls().stream())
                .anyMatch(matchingAcl -> matchingAcl.patternType() != PatternType.LITERAL.code());
            if (unsupported)
                throw new UnsupportedVersionException("Version 0 only supports literal resource pattern types");
        }

        final boolean unknown = filterResults().stream()
                .flatMap(r -> r.matchingAcls().stream())
                .anyMatch(matchingAcl -> matchingAcl.patternType() == PatternType.UNKNOWN.code()
                    || matchingAcl.resourceType() == ResourceType.UNKNOWN.code()
                    || matchingAcl.permissionType() == AclPermissionType.UNKNOWN.code()
                    || matchingAcl.operation() == AclOperation.UNKNOWN.code());
        if (unknown)
            throw new IllegalArgumentException("DeleteAclsMatchingAcls contain UNKNOWN elements");
    }

    public static DeleteAclsResponseData.DeleteAclsFilterResult filterResult(AclDeleteResult result) {
        ApiError error = result.exception().map(e -> ApiError.fromThrowable(e)).orElse(ApiError.NONE);
        List<DeleteAclsResponseData.DeleteAclsMatchingAcl> matchingAcls = result.aclBindingDeleteResults().stream()
            .map(DeleteAclsResponse::matchingAcl)
            .collect(Collectors.toList());
        return new DeleteAclsResponseData.DeleteAclsFilterResult()
            .setErrorCode(error.error().code())
            .setErrorMessage(error.message())
            .setMatchingAcls(matchingAcls);
    }

    private static DeleteAclsResponseData.DeleteAclsMatchingAcl matchingAcl(AclDeleteResult.AclBindingDeleteResult result) {
        ApiError error = result.exception().map(e -> ApiError.fromThrowable(e)).orElse(ApiError.NONE);
        AclBinding acl = result.aclBinding();
        return matchingAcl(acl, error);
    }

    // Visible for testing
    public static DeleteAclsResponseData.DeleteAclsMatchingAcl matchingAcl(AclBinding acl, ApiError error) {
        return new DeleteAclsResponseData.DeleteAclsMatchingAcl()
            .setErrorCode(error.error().code())
            .setErrorMessage(error.message())
            .setResourceName(acl.pattern().name())
            .setResourceType(acl.pattern().resourceType().code())
            .setPatternType(acl.pattern().patternType().code())
            .setHost(acl.entry().host())
            .setOperation(acl.entry().operation().code())
            .setPermissionType(acl.entry().permissionType().code())
            .setPrincipal(acl.entry().principal());
    }

    public static AclBinding aclBinding(DeleteAclsResponseData.DeleteAclsMatchingAcl matchingAcl) {
        ResourcePattern resourcePattern = new ResourcePattern(ResourceType.fromCode(matchingAcl.resourceType()),
            matchingAcl.resourceName(), PatternType.fromCode(matchingAcl.patternType()));
        AccessControlEntry accessControlEntry = new AccessControlEntry(matchingAcl.principal(), matchingAcl.host(),
            AclOperation.fromCode(matchingAcl.operation()), AclPermissionType.fromCode(matchingAcl.permissionType()));
        return new AclBinding(resourcePattern, accessControlEntry);
    }

}
