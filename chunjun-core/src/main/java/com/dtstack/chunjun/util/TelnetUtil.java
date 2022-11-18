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

package com.dtstack.chunjun.util;

import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.telnet.TelnetClient;

import java.net.InetAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class TelnetUtil {

    private static final Pattern JDBC_PATTERN = Pattern.compile("(?<host>[^:@/]+):(?<port>\\d+).*");
    public static final String PHOENIX_PREFIX = "jdbc:phoenix";
    private static final Pattern PHOENIX_PATTERN =
            Pattern.compile("jdbc:phoenix:(?<host>\\S+):(?<port>\\d+).*");
    private static final String HOST_KEY = "host";
    private static final String PORT_KEY = "port";
    private static final String SPLIT_KEY = ",";

    public static boolean telnet(String ip, int port) {
        boolean result = false;
        try {
            result =
                    RetryUtil.executeWithRetry(
                            () -> {
                                TelnetClient client = null;
                                try {
                                    client = new TelnetClient();
                                    client.setConnectTimeout(3000);
                                    client.connect(ip, port);
                                    return true;
                                } catch (Exception e) {
                                    throw new ChunJunRuntimeException(
                                            "Unable connect to : " + ip + ":" + port);
                                } finally {
                                    try {
                                        if (client != null) {
                                            client.disconnect();
                                        }
                                    } catch (Exception ignore) {
                                    }
                                }
                            },
                            3,
                            1000,
                            false);
        } catch (Exception e) {
            log.warn("", e);
        }
        return result;
    }

    public static void telnet(String url) {
        if (url == null || url.trim().length() == 0) {
            throw new IllegalArgumentException("url can not be null");
        }

        String host = null;
        int port = 0;
        Matcher matcher = null;
        if (StringUtils.startsWith(url, PHOENIX_PREFIX)) {
            matcher = PHOENIX_PATTERN.matcher(url);
        } else {
            matcher = JDBC_PATTERN.matcher(url);
        }
        if (matcher.find()) {
            host = matcher.group(HOST_KEY);
            port = Integer.parseInt(matcher.group(PORT_KEY));
        }

        if (host == null || port == 0) {
            // oracle高可用jdbc url此处获取不到IP端口，直接return。
            return;
        }

        if (host.contains(SPLIT_KEY)) {
            String[] hosts = host.split(SPLIT_KEY);
            for (String s : hosts) {
                if (StringUtils.isNotBlank(s)) {
                    telnet(s, port);
                }
            }
        } else {
            telnet(host, port);
        }
    }

    public static boolean ping(String ip) {
        try {
            return InetAddress.getByName(ip).isReachable(3000);
        } catch (Exception e) {
            return false;
        }
    }
}
