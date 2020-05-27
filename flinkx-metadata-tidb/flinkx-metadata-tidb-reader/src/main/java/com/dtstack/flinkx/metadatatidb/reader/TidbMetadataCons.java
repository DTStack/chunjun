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
package com.dtstack.flinkx.metadatatidb.reader;

/**
 * @author : kunni@dtstack.com
 * @date : 2020/5/26
 */
public class TidbMetadataCons {

    public static final String DRIVERNAME = "com.mysql.jdbc.Driver";
    public static final String KEY_TABLEPROPERTIES =  "tableProperties";
    public static final String KEY_TOTALSIZE = "totalSize";
    public static final String KEY_CREATETIME = "createTime";
    public static final String KEY_COLUMN = "column";
    public static final String KEY_NAME = "name";
    public static final String KEY_TYPE = "type";
    public static final String KEY_INDEX = "index";
    public static final String KEY_COMMENT = "comment";
    public static final String KEY_PARTITION = "partition";
    public static final String KEY_PARTITIONCOLUMN = "partitionColumn";
    public static final String KEY_ROWS = "rows";
    public static final String KEY_DEFAULT = "default";
    public static final String KEY_NULL = "null";
    public static final String KEY_HEALTHY = "healthy";

    public static final String KEY_TABLE_COMMENT = "Comment";
    public static final String KEY_TABLE_ROWS = "Rows";

    public static final String KEY_DATA_LENGTH = "Data_length";
    public static final String KEY_COLUMN_CREATE_TIME = "Create_time";
    public static final String KEY_UPDATE_TIME = "Update_time";
    public static final String KEY_COLUMN_COMMENT = "Comment";
    public static final String KEY_FIELD = "Field";
    public static final String KEY_COLUMN_TYPE = "Type";
    public static final String KEY_COLUMN_NULL = "Null";
    public static final String KEY_COLUMN_DEFAULT = "Default";
    public static final String KEY_PARTITION_NAME = "PARTITION_NAME";
    public static final String KEY_PARTITION_CREATE_TIME = "CREATE_TIME";
    public static final String KEY_PARTITION_TABLE_ROWS = "TABLE_ROWS";
    public static final String KEY_PARTITION_DATA_LENGTH = "DATA_LENGTH";
    public static final String KEY_PARTITIONNAME = "Partition_name";
    public static final String KEY_HEALTHY_HEALTHY = "Healthy";
    public static final String KEY_PARTITION_EXPRESSION = "PARTITION_EXPRESSION";

    /** sql语句 */
    public static final String SQL_SWITCH_DATABASE = "USE %s";
    public static final String SQL_QUERY_TABLEINFO = "SHOW TABLE STATUS LIKE '%s'";
    public static final String SQL_QUERY_COLUMN = "SHOW FULL COLUMNS FROM %s";
    public static final String SQL_QUERY_HEALTHY = "SHOW STATS_HEALTHY WHERE Table_name='%s' AND Db_name = schema()";
    public static final String SQL_QUERY_UPDATETIME = "SHOW STATS_META WHERE Table_name = '%s'";
    public static final String SQL_QUERY_PARTITION = "SELECT * FROM information_schema.partitions WHERE table_schema = schema() AND table_name='%s'";
    public static final String SQL_QUERY_PARTITIONCOLUMN = "SELECT DISTINCT PARTITION_EXPRESSION FROM information_schema.partitions WHERE table_schema = schema() AND table_name='%s'";
}
