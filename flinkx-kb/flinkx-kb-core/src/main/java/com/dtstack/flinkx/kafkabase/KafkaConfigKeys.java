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

package com.dtstack.flinkx.kafkabase;

/**
 * Date: 2019/11/21
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaConfigKeys {

    public static final String KEY_TOPIC = "topic";
    public static final String KEY_GROUP_ID = "groupId";
    public static final String KEY_TIMEZONE = "timezone";
    public static final String KEY_CODEC = "codec";
    public static final String KEY_BLANK_IGNORE = "blankIgnore";
    public static final String KEY_CONSUMER_SETTINGS = "consumerSettings";
    public static final String KEY_PRODUCER_SETTINGS = "producerSettings";
    public static final String KEY_TABLE_FIELDS = "tableFields";
    public static final String KEY_MODE = "mode";
    //partition:0,offset:42;partition:1,offset:300
    public static final String KEY_OFFSET = "offset";
    public static final String KEY_TIMESTAMP = "timestamp";

    public static final String GROUP_ID = "group.id";

    public static final String BOOTSTRAP_SERVERS= "bootstrap.servers";
}
