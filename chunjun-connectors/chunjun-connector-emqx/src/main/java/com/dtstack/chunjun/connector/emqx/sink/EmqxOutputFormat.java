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

package com.dtstack.chunjun.connector.emqx.sink;

import com.dtstack.chunjun.connector.emqx.config.EmqxConfig;
import com.dtstack.chunjun.connector.emqx.util.MqttConnectUtil;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.time.LocalTime;

import static com.dtstack.chunjun.connector.emqx.options.EmqxOptions.CLIENT_ID_WRITER;

@Slf4j
public class EmqxOutputFormat extends BaseRichOutputFormat {

    private static final long serialVersionUID = 2234010364657826897L;

    /** emqx Conf */
    private EmqxConfig emqxConfig;
    /** emqx client */
    private transient MqttClient client;

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        client =
                MqttConnectUtil.getMqttClient(
                        emqxConfig,
                        CLIENT_ID_WRITER.defaultValue() + LocalTime.now().toSecondOfDay() + jobId);

        client.setCallback(
                new MqttCallback() {

                    @Override
                    public void connectionLost(Throwable cause) {
                        log.warn("connection lost and reconnect , e = {}", cause.getMessage());
                        if (client != null && client.isConnected()) {
                            try {
                                client.disconnect();
                            } catch (MqttException e) {
                                log.error(e.getMessage());
                            }
                        }

                        try {
                            client = MqttConnectUtil.getMqttClient(emqxConfig, jobId);
                        } catch (Exception e) {
                            log.error(
                                    e.getMessage()
                                            + "\n"
                                            + " can not reconnect success, please restart job!!!");
                        }
                    }

                    @Override
                    public void messageArrived(String s, MqttMessage mqttMessage) {}

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {}
                });
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        try {
            MqttMessage message = (MqttMessage) rowConverter.toExternal(rowData, new MqttMessage());
            message.setQos(emqxConfig.getQos());
            client.publish(emqxConfig.getTopic(), message);
        } catch (Exception e) {
            throw new WriteRecordException("", e, 0, rowData);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void closeInternal() {
        MqttConnectUtil.close(client);
    }

    public EmqxConfig getEmqxConf() {
        return emqxConfig;
    }

    public void setEmqxConf(EmqxConfig emqxConfig) {
        this.emqxConfig = emqxConfig;
    }
}
