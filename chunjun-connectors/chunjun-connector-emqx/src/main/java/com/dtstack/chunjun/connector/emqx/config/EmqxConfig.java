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
package com.dtstack.chunjun.connector.emqx.config;

import com.dtstack.chunjun.config.CommonConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class EmqxConfig extends CommonConfig {

    private static final long serialVersionUID = -1825374657637975204L;

    /** emq address:tcp://localhost:1883 */
    private String broker;
    /** emq topic */
    private String topic;
    /** emq username */
    private String username;
    /** emq password */
    private String password;
    /** emq clean session */
    private boolean isCleanSession = true;
    /** emq EXACTLY_ONCE */
    private int qos = 2;
    /** emq codec */
    private String codec = "plain";
    /** emqx reconnect times */
    private int connectRetryTimes = 10;

    /**
     * Field mapping configuration. The data passed from the reader plug-in to the writer plug-in
     * only contains its value attribute. After configuring this parameter, it can be restored to a
     * key-value pair type json string output
     */
    private List<String> tableFields;
}
