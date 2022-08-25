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

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.time.LocalTime;

import static com.dtstack.chunjun.connector.emqx.options.EmqxOptions.CLIENT_ID_WRITER;

/**
 * @author chuixue
 * @create 2021-06-01 20:09
 * @description
 */
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
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        try {
            MqttMessage message = (MqttMessage) rowConverter.toExternal(rowData, new MqttMessage());
            message.setQos(emqxConf.getQos());
            client.publish(emqxConf.getTopic(), message);
        } catch (MqttException e) {
            //当mqtt客户端连接异常报错的时候，再重新建立连接
            if (this.client == null || !this.client.isConnected()) {
                // 如果关闭的话，就重新连接
                MqttConnectUtil.getMqttClient(
                        emqxConf,
                        CLIENT_ID_WRITER.defaultValue() + LocalTime.now().toSecondOfDay() + jobId);
            }
            throw new RuntimeException(e);
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
