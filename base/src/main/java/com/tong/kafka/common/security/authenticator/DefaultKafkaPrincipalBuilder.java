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
package com.tong.kafka.common.security.authenticator;

import com.tong.kafka.common.config.SaslConfigs;
import com.tong.kafka.common.errors.SerializationException;
import com.tong.kafka.common.protocol.ByteBufferAccessor;
import com.tong.kafka.common.security.auth.*;
import com.tong.kafka.common.security.kerberos.KerberosName;
import com.tong.kafka.common.KafkaException;
import com.tong.kafka.common.message.DefaultPrincipalData;
import com.tong.kafka.common.protocol.MessageUtil;

import com.tong.kafka.common.security.kerberos.KerberosShortNamer;
import com.tong.kafka.common.security.ssl.SslPrincipalMapper;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Principal;

/**
 * Default implementation of {@link KafkaPrincipalBuilder} which provides basic support for
 * SSL authentication and SASL authentication. In the latter case, when GSSAPI is used, this
 * class applies {@link KerberosShortNamer} to transform
 * the name.
 *
 * NOTE: This is an internal class and can change without notice.
 */
public class DefaultKafkaPrincipalBuilder implements KafkaPrincipalBuilder, KafkaPrincipalSerde {
    private final KerberosShortNamer kerberosShortNamer;
    private final SslPrincipalMapper sslPrincipalMapper;

    /**
     * Construct a new instance.
     *
     * @param kerberosShortNamer Kerberos name rewrite rules or null if none have been configured
     * @param sslPrincipalMapper SSL Principal mapper or null if none have been configured
     */
    public DefaultKafkaPrincipalBuilder(KerberosShortNamer kerberosShortNamer, SslPrincipalMapper sslPrincipalMapper) {
        this.kerberosShortNamer = kerberosShortNamer;
        this.sslPrincipalMapper = sslPrincipalMapper;
    }

    @Override
    public KafkaPrincipal build(AuthenticationContext context) {
        if (context instanceof PlaintextAuthenticationContext) {
            return KafkaPrincipal.ANONYMOUS;
        } else if (context instanceof SslAuthenticationContext) {
            SSLSession sslSession = ((SslAuthenticationContext) context).session();
            try {
                return applySslPrincipalMapper(sslSession.getPeerPrincipal());
            } catch (SSLPeerUnverifiedException se) {
                return KafkaPrincipal.ANONYMOUS;
            }
        } else if (context instanceof SaslAuthenticationContext) {
            SaslServer saslServer = ((SaslAuthenticationContext) context).server();
            if (SaslConfigs.GSSAPI_MECHANISM.equals(saslServer.getMechanismName()))
                return applyKerberosShortNamer(saslServer.getAuthorizationID());
            else
                return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, saslServer.getAuthorizationID());
        } else {
            throw new IllegalArgumentException("Unhandled authentication context type: " + context.getClass().getName());
        }
    }

    private KafkaPrincipal applyKerberosShortNamer(String authorizationId) {
        KerberosName kerberosName = KerberosName.parse(authorizationId);
        try {
            String shortName = kerberosShortNamer.shortName(kerberosName);
            return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, shortName);
        } catch (IOException e) {
            throw new KafkaException("Failed to set name for '" + kerberosName +
                    "' based on Kerberos authentication rules.", e);
        }
    }

    private KafkaPrincipal applySslPrincipalMapper(Principal principal) {
        try {
            if (!(principal instanceof X500Principal) || principal == KafkaPrincipal.ANONYMOUS) {
                return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, principal.getName());
            } else {
                return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, sslPrincipalMapper.getName(principal.getName()));
            }
        } catch (IOException e) {
            throw new KafkaException("Failed to map name for '" + principal.getName() +
                    "' based on SSL principal mapping rules.", e);
        }
    }

    @Override
    public byte[] serialize(KafkaPrincipal principal) {
        DefaultPrincipalData data = new DefaultPrincipalData()
                                        .setType(principal.getPrincipalType())
                                        .setName(principal.getName())
                                        .setTokenAuthenticated(principal.tokenAuthenticated());
        return MessageUtil.toVersionPrefixedBytes(DefaultPrincipalData.HIGHEST_SUPPORTED_VERSION, data);
    }

    @Override
    public KafkaPrincipal deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        short version = buffer.getShort();
        if (version < DefaultPrincipalData.LOWEST_SUPPORTED_VERSION || version > DefaultPrincipalData.HIGHEST_SUPPORTED_VERSION) {
            throw new SerializationException("Invalid principal data version " + version);
        }

        DefaultPrincipalData data = new DefaultPrincipalData(new ByteBufferAccessor(buffer), version);
        return new KafkaPrincipal(data.type(), data.name(), data.tokenAuthenticated());
    }
}
