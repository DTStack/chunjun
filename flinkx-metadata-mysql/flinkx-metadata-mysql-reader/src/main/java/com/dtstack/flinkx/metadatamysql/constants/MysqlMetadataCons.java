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

package com.dtstack.flinkx.metadatamysql.constants;

import com.dtstack.metadata.rdb.core.constants.RdbCons;

/**
 * 由于查询语句有变化，MysqlMetadataCons改写了一些常量定义
 * @author : kunni@dtstack.com
 * @date : 2020/6/8
 */

public class MysqlMetadataCons extends RdbCons {

    public static final String DRIVER_NAME = "com.mysql.jdbc.Driver";

    public static final String KEY_ENGINE = "engine";
    public static final String KEY_ROW_FORMAT = "rowFormat";
    public static final String KEY_TABLE_TYPE = "tableType";

    public static final String RESULT_TABLE_TYPE = "TABLE_TYPE";
    public static final String RESULT_ENGINE = "ENGINE";
    public static final String RESULT_ROW_FORMAT = "ROW_FORMAT";
    public static final String RESULT_ROWS = "TABLE_ROWS";
    public static final String RESULT_DATA_LENGTH = "DATA_LENGTH";
    public static final String RESULT_CREATE_TIME = "CREATE_TIME";
    public static final String RESULT_TABLE_COMMENT = "TABLE_COMMENT";
    public static final String RESULT_KEY_NAME = "Key_name";
    public static final String RESULT_COLUMN_NAME = "Column_name";
    public static final String RESULT_INDEX_TYPE = "Index_type";
    public static final String RESULT_INDEX_COMMENT = "Index_comment";

    public static final String SQL_QUERY_TABLE_INFO = "SELECT * FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'";
    public static final String SQL_SWITCH_DATABASE = "USE `%s`";
    public static final String SQL_QUERY_INDEX = "SHOW INDEX FROM `%s`";
}
