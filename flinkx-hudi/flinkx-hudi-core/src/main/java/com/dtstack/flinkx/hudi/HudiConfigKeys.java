package com.dtstack.flinkx.hudi;/*
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

/**
 * @author fengjiangtao_yewu@cmss.chinamobile.com
 * @date 2021-08-05
 */
public class HudiConfigKeys {
    public static final String KEY_PATH = "path";

    public static final String KEY_HADOOP_CONFIG = "hadoopConfig";

    public static final String KEY_DEFAULT_FS = "defaultFS";

    public static final String KEY_WRITE_MODE = "writeMode";

    public static final String KEY_COLUMN_NAME = "name";

    public static final String KEY_COLUMN_TYPE = "type";

    public static final String KEY_COMPRESS = "compress";

    public static final String KEY_TABLE_NAME = "tableName";

    public static final String KEY_TABLE_TYPE = "tableType";

    public static final String KEY_TABLE_RECORD_KEY = "recordKey";

    public static final String KEY_TABLE_TYPE_RECORD = "record";

    public static final String KEY_SCHEMA_FIELDS = "fields";

    public static final String KEY_PARTITION_FIELDS = "partitionFields";

    public static final String KEY_HIVE_JDBC = "hiveJdbcUrl";

    public static final String KEY_HIVE_METASTORE = "hiveMetastore";

    public static final String KEY_HIVE_USER = "hiveUser";

    public static final String KEY_HIVE_PASS = "hivePass";

    public static final String KEY_BATCH_INTERVAL = "batchInterval";

    public static final String KEY_HA_DEFAULT_FS = "dfs.nameservices";

    public static final String KEY_HIVE_METASTORE_URIS = "hive.metastore.uris";

    public static final String KEY_LAKEHOUSE_METADATAURL = "lakehouse.metadataUrl";

    public static final String KEY_LAKEHOUSE_JOBUUID = "lakehouse.jobUUID";

    public static final String KEY_LAKEHOUSE_USERID = "lakehouse.userId";

    public static final String KEY_HADOOP_USER_NAME = "HADOOP_USER_NAME";

    public static final String KEY_DBTABLE_DELIMITER = ".";
}
