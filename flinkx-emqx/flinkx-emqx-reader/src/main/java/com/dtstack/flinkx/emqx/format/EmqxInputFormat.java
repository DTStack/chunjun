/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.emqx.format;

import com.dtstack.flinkx.decoder.IDecode;
import com.dtstack.flinkx.decoder.JsonDecoder;
import com.dtstack.flinkx.decoder.TextDecoder;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * Date: 2020/02/12
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class EmqxInputFormat extends BaseRichInputFormat {
    private static final Logger LOG = LoggerFactory.getLogger(EmqxInputFormat.class);

    private static final String DEFAULT_CODEC = "json";
    private static final String CLIENT_ID_PRE = "reader";

    public String broker;
    public String topic;
    public String username;
    public String password;
    public String codec;
    public boolean isCleanSession;
    public int qos;

    private transient IDecode decode;
    private transient MqttClient client;
    private transient BlockingQueue<Row> queue;

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        super.openInputFormat();
        if (DEFAULT_CODEC.equals(codec)) {
            decode = new JsonDecoder();
        } else {
            decode = new TextDecoder();
        }

        try {
            queue = new SynchronousQueue<>(false);
            client = new MqttClient(broker, CLIENT_ID_PRE + jobId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(isCleanSession);
            if(StringUtils.isNotBlank(username)){
                options.setUserName(username);
                options.setPassword(password.toCharArray());
            }
            options.setAutomaticReconnect(true);

            client.setCallback(new MqttCallback() {

                @Override
                public void connectionLost(Throwable cause) {
                    LOG.warn("connection lost, e = {}", ExceptionUtil.getErrorMessage(cause));
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    String msg = new String(message.getPayload());
                    if(LOG.isTraceEnabled()){
                        LOG.trace("msg = {}", msg);
                    }
                    Map<String, Object> event = decode.decode(msg);
                    if (event != null && event.size() > 0) {
                        processEvent(event);
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    if(LOG.isDebugEnabled()){
                        LOG.debug("deliveryComplete = {}", token.isComplete());
                    }
                }
            });
            client.connect(options);
            client.subscribe(topic, qos);
            LOG.info("emqx is connected = {} ", client.isConnected());
        }catch (MqttException e){
            LOG.error("reason = {}, msg = {}, loc = {}, cause = {}, e = {}",
                    e.getReasonCode(),
                    e.getMessage(),
                    e.getLocalizedMessage(),
                    e.getCause(), e);
        }
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        try {
            row = queue.take();
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error:{}", ExceptionUtil.getErrorMessage(e));
        }
        return row;
    }

    @Override
    public void closeInternal() throws IOException {
        try {
            client.disconnect();
        } catch (MqttException e) {
            LOG.error("error to disconnect emqx client, e = {}", ExceptionUtil.getErrorMessage(e));
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    public void processEvent(Map<String, Object> event) {
        try {
            queue.put(Row.of(event));
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted event:{} error:{}", event, e);
        }
    }

}
