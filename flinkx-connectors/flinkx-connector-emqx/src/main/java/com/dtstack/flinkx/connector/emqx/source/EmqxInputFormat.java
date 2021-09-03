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

package com.dtstack.flinkx.connector.emqx.source;

import com.dtstack.flinkx.connector.emqx.conf.EmqxConf;
import com.dtstack.flinkx.connector.emqx.util.MqttConnectUtil;
import com.dtstack.flinkx.source.format.BaseRichInputFormat;
import com.dtstack.flinkx.throwable.ReadRecordException;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import static com.dtstack.flinkx.connector.emqx.options.EmqxOptions.CLIENT_ID_READER;

/**
 * @author chuixue
 * @create 2021-06-01 20:09
 * @description
 */
public class EmqxInputFormat extends BaseRichInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(EmqxInputFormat.class);

    /** emqx Conf */
    private EmqxConf emqxConf;
    /** emqx client */
    private transient MqttClient client;
    /** Encapsulate the data in emq */
    private transient BlockingQueue<String> queue;

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        super.openInputFormat();

        try {
            queue = new SynchronousQueue<>(false);
            client =
                    MqttConnectUtil.getMqttClient(
                            emqxConf,
                            CLIENT_ID_READER.defaultValue()
                                    + LocalTime.now().toSecondOfDay()
                                    + jobId);
            client.setCallback(
                    new MqttCallback() {

                        @Override
                        public void connectionLost(Throwable cause) {
                            LOG.warn("connection lost and reconnect , e = {}", cause.getMessage());

                            if (client != null && client.isConnected()) {
                                try {
                                    client.disconnect();
                                } catch (MqttException e) {
                                    LOG.error(e.getMessage());
                                }
                            }

                            try {
                                client = MqttConnectUtil.getMqttClient(emqxConf, jobId);
                            } catch (Exception e) {
                                LOG.error(
                                        e.getMessage()
                                                + "\n"
                                                + " can not reconnect success, please restart job!!!");
                            }
                        }

                        @Override
                        public void messageArrived(String topic, MqttMessage message) {
                            String msg = new String(message.getPayload(), StandardCharsets.UTF_8);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("msg = {}", msg);
                            }
                            try {
                                queue.put(msg);
                            } catch (InterruptedException e) {
                                LOG.error(e.getMessage() + "\n" + msg);
                            }
                        }

                        @Override
                        public void deliveryComplete(IMqttDeliveryToken token) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("deliveryComplete = {}", token.isComplete());
                            }
                        }
                    });
            client.subscribe(emqxConf.getTopic(), emqxConf.getQos());
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        try {
            rowData = rowConverter.toInternal(queue.take());
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
        return rowData;
    }

    @Override
    protected void closeInternal() {
        MqttConnectUtil.close(client);
    }

    @Override
    public boolean reachedEnd() {
        return false;
    }

    public void setEmqxConf(EmqxConf emqxConf) {
        this.emqxConf = emqxConf;
    }

    public EmqxConf getEmqxConf() {
        return emqxConf;
    }
}
