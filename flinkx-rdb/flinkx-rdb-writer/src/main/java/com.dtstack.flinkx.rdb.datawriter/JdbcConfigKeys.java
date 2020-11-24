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

package com.dtstack.flinkx.rdb.datawriter;

/**
 * Configuration Keys for JdbcDataWriter
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class JdbcConfigKeys {

    public static final String KEY_WRITE_MODE = "writeMode";
    public static final String KEY_USERNAME = "username";
    public static final String KEY_PASSWORD = "password";
    public static final String KEY_PRE_SQL = "preSql";
    public static final String KEY_POST_SQL = "postSql";
    public static final String KEY_BATCH_SIZE = "batchSize";
    public static final String KEY_UPDATE_KEY = "updateKey";
    public static final String KEY_FULL_COLUMN = "fullColumn";
    public static final String KEY_INSERT_SQL_MODE = "insertSqlMode";
    public static final String KEY_PROPERTIES = "properties";
}
