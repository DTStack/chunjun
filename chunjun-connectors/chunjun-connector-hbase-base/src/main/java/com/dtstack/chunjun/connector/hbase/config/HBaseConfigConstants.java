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

package com.dtstack.chunjun.connector.hbase.config;

/** The class containing Hbase configuration constants */
public class HBaseConfigConstants {

    public static final int DEFAULT_SCAN_CACHE_SIZE = 256;

    public static final int MAX_SCAN_CACHE_SIZE = 1000;

    public static final int MIN_SCAN_CACHE_SIZE = 1;

    public static final String DEFAULT_ENCODING = "UTF-8";

    public static final String DEFAULT_DATA_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final String DEFAULT_NULL_MODE = "skip";

    public static final long DEFAULT_WRITE_BUFFER_SIZE = 8 * 1024 * 1024L;

    public static final boolean DEFAULT_WAL_FLAG = false;

    public static final String MULTI_VERSION_FIXED_COLUMN = "multiVersionFixedColumn";
}
