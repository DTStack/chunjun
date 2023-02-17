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

package com.dtstack.chunjun.connector.http.common;

import com.google.common.collect.Sets;

import java.util.Set;

public class ConstantValue {

    public static final String STRATEGY_STOP = "stop";
    public static final String STRATEGY_RETRY = "retry";
    public static final String SYSTEM_FUNCTION_UUID = "uuid";
    public static final String SYSTEM_FUNCTION_CURRENT_TIME = "currentTime";
    public static final String SYSTEM_FUNCTION_INTERVAL_TIME = "intervalTime";

    public static final String CSV_DECODE = "csv";
    public static final String XML_DECODE = "xml";
    public static final String TEXT_DECODE = "text";

    public static final String DEFAULT_DECODE = "json";

    public static final String PREFIX = "${";
    public static final String SUFFIX = "}";
    public static final String POINT_SYMBOL = ".";

    public static final String CONTENT_TYPE_NAME = "Content-Type";
    public static final String CONTENT_TYPE_DEFAULT_VALUE = "application/json";

    public static final int REQUEST_RETRY_TIME = 3;

    public static final Set<String> FIELD_DELIMITER = Sets.newHashSet(",", "_", "/", ".", "-", ":");
}
