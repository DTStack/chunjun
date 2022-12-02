/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.nebula.utils;

import com.dtstack.chunjun.connector.nebula.config.NebulaConfig;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.util.Preconditions;

import com.vesoft.nebula.client.graph.data.CASignedSSLParam;
import com.vesoft.nebula.client.graph.data.SSLParam;
import com.vesoft.nebula.client.graph.data.SelfSignedSSLParam;

public class GraphUtil {

    public static SSLParam getSslParam(NebulaConfig nebulaConfig) {
        SSLParam sslParam = null;
        if (nebulaConfig.getEnableSSL()) {
            switch (nebulaConfig.getSslParamType()) {
                case CA_SIGNED_SSL_PARAM:
                    checkSSL(nebulaConfig);
                    sslParam =
                            new CASignedSSLParam(
                                    nebulaConfig.getCaCrtFilePath(),
                                    nebulaConfig.getCrtFilePath(),
                                    nebulaConfig.getKeyFilePath());

                    break;
                case SELF_SIGNED_SSL_PARAM:
                    checkSSL(nebulaConfig);
                    sslParam =
                            new SelfSignedSSLParam(
                                    nebulaConfig.getCrtFilePath(),
                                    nebulaConfig.getKeyFilePath(),
                                    nebulaConfig.getPassword());
                    break;
                default:
                    throw new ChunJunRuntimeException(
                            "unsupport ssl type: " + nebulaConfig.getSslParamType());
            }
        }
        return sslParam;
    }

    public static void checkSSL(NebulaConfig nebulaConfig) {
        switch (nebulaConfig.getSslParamType()) {
            case CA_SIGNED_SSL_PARAM:
                Preconditions.checkNotNull(
                        nebulaConfig.getCaCrtFilePath(),
                        "nebula enableSSL is true,but caCrtFilePath is null!");
                Preconditions.checkNotNull(
                        nebulaConfig.getCrtFilePath(),
                        "nebula enableSSL is true,but crtFilePath is null!");
                Preconditions.checkNotNull(
                        nebulaConfig.getKeyFilePath(),
                        "nebula enableSSL is true,but keyFilePath is null!");

                break;
            case SELF_SIGNED_SSL_PARAM:
                Preconditions.checkNotNull(
                        nebulaConfig.getSslPassword(),
                        "nebula enableSSL is true,but ssl password is null!");
                Preconditions.checkNotNull(
                        nebulaConfig.getCrtFilePath(),
                        "nebula enableSSL is true,but crtFilePath is null!");
                Preconditions.checkNotNull(
                        nebulaConfig.getKeyFilePath(),
                        "nebula enableSSL is true,but keyFilePath is null!");
                break;
            default:
                throw new ChunJunRuntimeException(
                        "unsupport ssl type: " + nebulaConfig.getSslParamType());
        }
    }
}
