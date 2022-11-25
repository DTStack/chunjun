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

import com.dtstack.chunjun.connector.emqx.conf.EmqxConf;
import com.dtstack.chunjun.connector.emqx.util.MqttConnectUtil;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.time.LocalTime;

import static com.dtstack.chunjun.connector.emqx.options.EmqxOptions.CLIENT_ID_WRITER;

public class EmqxOutputFormat extends BaseRichOutputFormat {

    /** emqx Conf */
    private EmqxConf emqxConf;
    /** emqx client */
    private transient MqttClient client;

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        client =
                MqttConnectUtil.getMqttClient(
                        emqxConf,
                        CLIENT_ID_WRITER.defaultValue() + LocalTime.now().toSecondOfDay() + jobId);

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
                    public void messageArrived(String s, MqttMessage mqttMessage)
                            throws Exception {}

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {}
                });
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        try {
            MqttMessage message = (MqttMessage) rowConverter.toExternal(rowData, new MqttMessage());
            message.setQos(emqxConf.getQos());
            client.publish(emqxConf.getTopic(), message);
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

    public EmqxConf getEmqxConf() {
        return emqxConf;
    }

    public void setEmqxConf(EmqxConf emqxConf) {
        this.emqxConf = emqxConf;
    }
}
