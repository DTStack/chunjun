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

package com.dtstack.flinkx.hdfs;

/**
 * This class defines configuration keys for HdfsReader and HdfsWriter
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class HdfsConfigKeys {

    public static final String KEY_FIELD_DELIMITER = "fieldDelimiter";

    public static final String KEY_DEFAULT_FS = "defaultFS";

    public static final String KEY_PATH = "path";

    public static final String KEY_HADOOP_CONFIG = "hadoopConfig";

    public static final String KEY_FILTER = "filterRegex";

    public static final String KEY_FILE_TYPE = "fileType";

    public static final String KEY_WRITE_MODE = "writeMode";

    public static final String KEY_FULL_COLUMN_NAME_LIST = "fullColumnName";

    public static final String KEY_FULL_COLUMN_TYPE_LIST = "fullColumnType";

    public static final String KEY_COLUMN_NAME = "name";

    public static final String KEY_COLUMN_TYPE = "type";

    public static final String KEY_COMPRESS = "compress";

    public static final String KEY_FILE_NAME = "fileName";

    public static final String KEY_ENCODING = "encoding";

    public static final String KEY_ROW_GROUP_SIZE = "rowGroupSize";

    public static final String KEY_MAX_FILE_SIZE = "maxFileSize";

    public static final String KEY_FLUSH_INTERVAL = "flushInterval";

}
