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

import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import org.apache.commons.lang3.StringUtils;

/**
 * Date: 2020/02/12
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class EmqxOutputFormatBuilder extends BaseRichOutputFormatBuilder {
    private EmqxOutputFormat format;

    public EmqxOutputFormatBuilder(){
        super.format = format = new EmqxOutputFormat();
    }

    public EmqxOutputFormatBuilder setBroker(String broker) {
        format.broker = broker;
        return this;
    }

    public EmqxOutputFormatBuilder setTopic(String topic) {
        format.topic = topic;
        return this;
    }

    public EmqxOutputFormatBuilder setUsername(String username) {
        format.username = username;
        return this;
    }

    public EmqxOutputFormatBuilder setPassword(String password) {
        format.password = password;
        return this;
    }

    public EmqxOutputFormatBuilder setCleanSession(boolean cleanSession) {
        format.isCleanSession = cleanSession;
        return this;
    }

    public EmqxOutputFormatBuilder setQos(int qos) {
        format.qos = qos;
        return this;
    }

    @Override
    protected void checkFormat() {
        if(StringUtils.isBlank(format.broker)){
            throw new IllegalArgumentException("emqx broker cannot be blank");
        }
        if(StringUtils.isBlank(format.topic)){
            throw new IllegalArgumentException("emqx topic cannot be blank");
        }
    }
}
