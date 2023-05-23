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

package com.dtstack.chunjun.constants;

public class ConstantValue {

    public static final String STAR_SYMBOL = "*";
    public static final String POINT_SYMBOL = ".";
    public static final String TWO_POINT_SYMBOL = "..";
    public static final String EQUAL_SYMBOL = "=";
    public static final String COLON_SYMBOL = ":";
    public static final String SINGLE_QUOTE_MARK_SYMBOL = "'";
    public static final String DOUBLE_QUOTE_MARK_SYMBOL = "\"";
    public static final String COMMA_SYMBOL = ",";
    public static final String SEMICOLON_SYMBOL = ";";

    public static final String SINGLE_SLASH_SYMBOL = "/";
    public static final String DOUBLE_SLASH_SYMBOL = "//";

    public static final String LEFT_PARENTHESIS_SYMBOL = "(";
    public static final String RIGHT_PARENTHESIS_SYMBOL = ")";

    public static final String LESS_THAN_SIGN = "<";
    public static final String GREATER_THAN_SIGN = ">";

    public static final String DATA_TYPE_UNSIGNED = "UNSIGNED";
    public static final String DATA_TYPE_UNSIGNED_LOWER = "unsigned";

    public static final String KEY_HTTP = "http";

    public static final String PROTOCOL_HTTP = "http://";
    public static final String PROTOCOL_HTTPS = "https://";
    public static final String PROTOCOL_HDFS = "hdfs://";
    public static final String PROTOCOL_JDBC_MYSQL = "jdbc:mysql://";

    public static final String SYSTEM_PROPERTIES_KEY_OS = "os.name";
    public static final String SYSTEM_PROPERTIES_KEY_USER_DIR = "user.dir";
    public static final String SYSTEM_PROPERTIES_KEY_JAVA_VENDOR = "java.vendor";
    public static final String SYSTEM_PROPERTIES_KEY_FILE_ENCODING = "file.encoding";

    public static final String OS_WINDOWS = "windows";

    public static final String SHIP_FILE_PLUGIN_LOAD_MODE = "shipfile";
    public static final String CLASS_PATH_PLUGIN_LOAD_MODE = "classpath";

    public static final String TIME_SECOND_SUFFIX = "sss";
    public static final String TIME_MILLISECOND_SUFFIX = "SSS";

    public static final String FILE_SUFFIX_XML = ".xml";

    public static final int MAX_BATCH_SIZE = 200000;

    public static final long STORE_SIZE_G = 1024L * 1024 * 1024;

    public static final long STORE_SIZE_M = 1024L * 1024;

    public static final String CONNECTOR_DIR_NAME = "connector";

    public static final String FORMAT_DIR_NAME = "formats";

    public static final String DIRTY_DATA_DIR_NAME = "dirty-data-collector";

    public static final String RESTORE_DIR_NAME = "restore-plugins";

    public static final String DDL_DIR_NAME = "ddl";

    public static final String DDL_CONVENT_DIR_NAME = "ddl-plugins";
}
