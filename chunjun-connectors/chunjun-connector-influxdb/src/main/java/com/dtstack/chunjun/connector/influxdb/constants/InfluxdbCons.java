/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.connector.influxdb.constants;

public class InfluxdbCons {

    public static final String KEY_URL = "url";
    public static final String KEY_USER_NAME = "username";
    public static final String KEY_PASSWORD = "password";
    public static final String KEY_DATABASE = "database";
    public static final String KEY_MEASUREMENT = "measurement";
    public static final String KEY_FORMAT = "format";
    public static final String KEY_WHERE = "where";
    public static final String KEY_CUSTOM_SQL = "customSql";
    public static final String KEY_EPOCH = "epoch";
    public static final String KEY_SPLIT_KEY = "splitPk";
    public static final String KEY_QUERY_TIME_OUT = "queryTimeOut";
    public static final String KEY_FETCH_SIZE = "fetchSize";

    public static final String QUERY_FIELD = "show field keys from ${measurement}";
    public static final String QUERY_TAG = "show tag keys from ${measurement}";
}
