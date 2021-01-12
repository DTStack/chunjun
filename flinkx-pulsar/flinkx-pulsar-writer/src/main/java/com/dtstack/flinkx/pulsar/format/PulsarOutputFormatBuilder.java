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
package com.dtstack.flinkx.pulsar.format;

import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;

import java.util.Map;

/**
 * @author: pierre
 * @create: 2020/3/21
 */
public class PulsarOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private PulsarOutputFormat format;

    public PulsarOutputFormatBuilder() {
        super.format = format = new PulsarOutputFormat();
    }

    public void setTopic(String topic) {
        format.topic = topic;
    }

    public void setToken(String token) {
        format.token = token;
    }

    public void setPulsarServiceUrl(String pulsarServiceUrl) {
        format.pulsarServiceUrl = pulsarServiceUrl;
    }

    public void setProducerSettings(Map<String, Object> producerSettings) {
        format.producerSettings = producerSettings;
    }

    @Override
    protected void checkFormat() {

    }
}
