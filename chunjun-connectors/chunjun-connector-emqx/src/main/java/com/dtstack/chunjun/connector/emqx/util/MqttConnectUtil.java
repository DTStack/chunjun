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

package com.dtstack.chunjun.connector.emqx.util;

import com.dtstack.chunjun.connector.emqx.conf.EmqxConf;
import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author chuixue
 * @create 2021-06-04 10:12
 * @description
 */
public class MqttConnectUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MqttConnectUtil.class);

    /**
     * get MqttClient
     *
     * @param emqxConf conf
     * @param clientId id
     * @return MqttClient
     * @throws MqttException connect failed
     */
    public static MqttClient getMqttClient(EmqxConf emqxConf, String clientId) {
        MqttClient client = null;
        for (int i = 0; i <= 2; i++) {
            try {
                client = new MqttClient(emqxConf.getBroker(), clientId);
                MqttConnectOptions options = new MqttConnectOptions();
                options.setCleanSession(emqxConf.isCleanSession());
                if (StringUtils.isNotBlank(emqxConf.getUsername())) {
                    options.setUserName(emqxConf.getUsername());
                    options.setPassword(emqxConf.getPassword().toCharArray());
                }
                options.setAutomaticReconnect(true);

                LOG.info("connect " + (i + 1) + " times.");
                client.connect(options);
                LOG.info("emqx is connected = {} ", client.isConnected());

                if (client.isConnected()) {
                    break;
                }
            } catch (MqttException e) {
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException interruptedException) {
                    throw new RuntimeException(interruptedException);
                }
                if (i == 2) {
                    throw new RuntimeException(e);
                }
            }
        }
        return client;
    }

    public static void close(MqttClient client) {
        try {
            if (client != null && client.isConnected()) {
                client.disconnect();
            }
        } catch (MqttException e) {
            LOG.error("error to disconnect emqx client, e = {}", ExceptionUtil.getErrorMessage(e));
        }
    }
}
