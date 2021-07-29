
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
package com.dtstack.flinkx.hbase;

/**
 * This class defines configuration keys for HbaseReader and HbaseWriter
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HbaseConfigKeys {

    public static final String KEY_SCAN_CACHE_SIZE = "scanCacheSize";

    public static final String KEY_SCAN_BATCH_SIZE = "scanBatchSize";

    public static final String KEY_TABLE = "table";

    public static final String KEY_HBASE_CONFIG = "hbaseConfig";

    public static final String KEY_START_ROW_KEY = "startRowkey";

    public static final String KEY_END_ROW_KEY = "endRowkey";

    public static final String KEY_IS_BINARY_ROW_KEY = "isBinaryRowkey";

    public static final String KEY_ENCODING = "encoding";

    public static final String KEY_RANGE = "range";

    public static final String KEY_COLUMN_NAME = "name";

    public static final String KEY_COLUMN_TYPE = "type";

    public static final String KEY_ROW_KEY_COLUMN = "rowkeyColumn";

    public static final String KEY_ROW_KEY_COLUMN_INDEX = "index";

    public static final String KEY_ROW_KEY_COLUMN_TYPE = "type";

    public static final String KEY_ROW_KEY_COLUMN_VALUE = "value";

    public static final String KEY_NULL_MODE = "nullMode";

    public static final String KEY_WAL_FLAG = "walFlag";

    public static final String KEY_VERSION_COLUMN = "versionColumn";

    public static final String KEY_WRITE_BUFFER_SIZE = "writeBufferSize";

    public static final String KEY_VERSION_COLUMN_INDEX = "index";

    public static final String KEY_VERSION_COLUMN_VALUE = "value";


}
