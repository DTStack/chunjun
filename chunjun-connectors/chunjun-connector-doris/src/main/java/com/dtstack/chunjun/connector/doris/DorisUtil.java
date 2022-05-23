/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.doris;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;

public class DorisUtil {

    private static final String JDBC_QUERY_PORT = "9030";

    private static final String JDBC_TEMPLATE = "jdbc:mysql://%s:%s?useSSL=false";

    /**
     * split fe and get fe ip. like: http://172.16.21.193:8030
     *
     * @param fe fe
     * @return ip of fe.
     */
    private static String splitFe(String fe) {
        String[] split = fe.split("://");
        for (String s : split) {
            String[] items = s.split(":");
            if (items.length == 2) {
                return items[0];
            }
        }
        throw new RuntimeException("Get fe ip from fe uri: " + fe + " failed.");
    }

    public static String getJdbcUrlFromFe(List<String> feNodes, String url) {
        if (StringUtils.isEmpty(url)) {
            Collections.shuffle(feNodes);
            String fe = feNodes.get(0);
            String feIp = splitFe(fe);
            return String.format(JDBC_TEMPLATE, feIp, JDBC_QUERY_PORT);
        }
        return url;
    }

    public interface Accept<T> {
        void accept(T t) throws Exception;
    }

    public interface Action {
        void action() throws Exception;
    }

    public static <T> void doRetry(
            Accept<T> accept, Action action, T t, int retryTimes, long sleepTimeOut)
            throws Exception {
        for (int i = 0; i < retryTimes; i++) {
            try {
                accept.accept(t);
                return;
            } catch (Exception exception) {
                boolean retry = needRetry(exception);
                if (i + 1 == retryTimes || !retry) {
                    throw exception;
                }

                try {
                    Thread.sleep(sleepTimeOut);
                } catch (InterruptedException interruptedException) {
                    // ignore
                }
                action.action();
            }
        }
    }

    private static boolean needRetry(Exception exception) {
        if (exception != null) {
            String errorMessage = exception.getMessage();
            return StringUtils.isNotEmpty(errorMessage) && errorMessage.contains("err=-235");
        }
        return false;
    }
}
