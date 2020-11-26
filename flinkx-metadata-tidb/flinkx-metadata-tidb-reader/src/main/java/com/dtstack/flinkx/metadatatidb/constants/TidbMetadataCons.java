/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.metadatatidb.constants;

import com.dtstack.flinkx.metadata.MetaDataCons;

/**
 * @author : kunni@dtstack.com
 * @date : 2020/5/26
 */
public class TidbMetadataCons extends MetaDataCons {

    public static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
    public static final String KEY_TOTAL_SIZE = "totalSize";
    public static final String KEY_CREATE_TIME = "createTime";
    public static final String KEY_PARTITION_COLUMN = "partitionColumn";
    public static final String KEY_ROWS = "rows";
    public static final String KEY_DEFAULT = "default";
    public static final String KEY_NULL = "null";
    public static final String KEY_HEALTHY = "healthy";
    public static final String KEY_UPDATE_TIME = "updateTime";

    public static final String RESULT_ROWS = "Rows";
    public static final String RESULT_DATA_LENGTH = "Data_length";
    public static final String RESULT_FIELD = "Field";
    public static final String RESULT_TYPE = "Type";
    public static final String RESULT_COLUMN_NULL = "Null";
    public static final String RESULT_COLUMN_DEFAULT = "Default";
    public static final String RESULT_PARTITION_NAME = "PARTITION_NAME";
    public static final String RESULT_PARTITION_CREATE_TIME = "CREATE_TIME";
    public static final String RESULT_PARTITION_TABLE_ROWS = "TABLE_ROWS";
    public static final String RESULT_PARTITION_DATA_LENGTH = "DATA_LENGTH";
    public static final String RESULT_PARTITIONNAME = "Partition_name";
    public static final String RESULT_HEALTHY = "Healthy";
    public static final String RESULT_PARTITION_EXPRESSION = "PARTITION_EXPRESSION";
    public static final String RESULT_CREATE_TIME = "Create_time";
    public static final String RESULT_UPDATE_TIME = "Update_time";
    public static final String RESULT_COMMENT = "Comment";

    /** sql语句 */
    public static final String SQL_SWITCH_DATABASE = "USE `%s`";
    public static final String SQL_SHOW_TABLES = "SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'";
    public static final String SQL_QUERY_TABLE_INFO = "SHOW TABLE STATUS LIKE '%s'";
    public static final String SQL_QUERY_COLUMN = "SHOW FULL COLUMNS FROM %s";
    public static final String SQL_QUERY_HEALTHY = "SHOW STATS_HEALTHY WHERE Table_name='%s' AND Db_name = schema()";
    public static final String SQL_QUERY_UPDATE_TIME = "SHOW STATS_META WHERE Table_name = '%s'";
    public static final String SQL_QUERY_PARTITION = "SELECT * FROM information_schema.partitions WHERE table_schema = schema() AND table_name='%s'";
    public static final String SQL_QUERY_PARTITION_COLUMN = "SELECT DISTINCT PARTITION_EXPRESSION FROM information_schema.partitions WHERE table_schema = schema() AND table_name='%s'";
}

