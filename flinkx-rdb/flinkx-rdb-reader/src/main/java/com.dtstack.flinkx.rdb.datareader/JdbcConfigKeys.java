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

package com.dtstack.flinkx.rdb.datareader;

/**
 * Configuration Keys for JdbcDataReader
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class JdbcConfigKeys {

    public static final String KEY_SPLIK_KEY = "splitPk";

    public static final String KEY_USER_NAME = "username";

    public static final String KEY_PASSWORD = "password";

    public static final String KEY_WHERE = "where";

    public static final String KEY_FETCH_SIZE = "fetchSize";

    public static final String KEY_QUERY_TIME_OUT = "queryTimeOut";

    public static final String KEY_REQUEST_ACCUMULATOR_INTERVAL = "requestAccumulatorInterval";

    public static final String KEY_INCRE_COLUMN = "increColumn";

    public static final String KEY_START_LOCATION = "startLocation";

    public static final String KEY_CUSTOM_SQL = "customSql";

    public static final String KEY_ORDER_BY_COLUMN = "orderByColumn";

    public static final String KEY_USE_MAX_FUNC = "useMaxFunc";

    public static final String KEY_POLLING = "polling";

    public static final String KEY_POLLING_INTERVAL = "pollingInterval";

    public static final String KEY_PROPERTIES = "properties";
}
