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

import com.dtstack.flinkx.decoder.JsonDecoder;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.MapUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Date: 2020/02/12
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class EmqxOutputFormat extends BaseRichOutputFormat {
    private static final Logger LOG = LoggerFactory.getLogger(EmqxOutputFormat.class);
    private static final String CLIENT_ID_PRE = "writer";

    public String broker;
    public String topic;
    public String username;
    public String password;
    public boolean isCleanSession;
    public int qos;

    private transient MqttClient client;
    protected static JsonDecoder jsonDecoder = new JsonDecoder();


    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        try {
            client = new MqttClient(broker, CLIENT_ID_PRE + jobId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(isCleanSession);
            if(StringUtils.isNotBlank(username)){
                options.setUserName(username);
                options.setPassword(password.toCharArray());
            }
            options.setAutomaticReconnect(true);
            client.connect(options);
            LOG.info("emqx is connected = {} ", client.isConnected());
        } catch (MqttException e) {
            LOG.error("reason = {}, msg = {}, loc = {}, cause = {}, e = {}",
                    e.getReasonCode(),
                    e.getMessage(),
                    e.getLocalizedMessage(),
                    e.getCause(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        try {
            Map<String, Object> map;
            if(row.getArity() == 1){
                Object obj = row.getField(0);
                if (obj instanceof Map) {
                    map = (Map<String, Object>) obj;
                } else if (obj instanceof String) {
                    map = jsonDecoder.decode(obj.toString());
                } else {
                    map = Collections.singletonMap("message", row.toString());
                }
            }else{
                map = Collections.singletonMap("message", row.toString());
            }
            MqttMessage message = new MqttMessage(MapUtil.writeValueAsString(map).getBytes());
            message.setQos(qos);
            client.publish(topic, message);
        } catch (Throwable e) {
            LOG.error("emqx writeSingleRecordInternal error:{}", ExceptionUtil.getErrorMessage(e));
            throw new WriteRecordException(e.getMessage(), e);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean isStreamButNoWriteCheckpoint() {
        return true;
    }
}
