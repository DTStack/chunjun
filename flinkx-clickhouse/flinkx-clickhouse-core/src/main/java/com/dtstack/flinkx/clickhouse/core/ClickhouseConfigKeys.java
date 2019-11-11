/**
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
package com.dtstack.flinkx.clickhouse.core;

/**
 * Date: 2019/11/05
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class ClickhouseConfigKeys {
    public final static String KEY_URL = "url";
    public final static String KEY_USERNAME = "username";
    public final static String KEY_PASSWORD = "password";
    public final static String KEY_TABLE = "table";
    public final static String KEY_CLICKHOUSE_PROP = "clickhouseProp";
    public static final String KEY_QUERY_TIME_OUT = "queryTimeOut";
    public final static String KEY_SPLITKEY = "splitKey";
    public final static String KEY_FILTER = "filter";
    public final static String KEY_INCRE_COLUMN = "increColumn";
    public final static String KEY_START_LOCATION = "startLocation";
    public final static String KEY_BATCH_INTERVAL = "batchInterval";
    public final static String KEY_PRESQL = "preSql";
    public final static String KEY_POSTSQL = "postSql";
}
