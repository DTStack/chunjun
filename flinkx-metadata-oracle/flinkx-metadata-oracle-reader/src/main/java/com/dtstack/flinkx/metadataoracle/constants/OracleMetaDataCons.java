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

package com.dtstack.flinkx.metadataoracle.constants;

import com.dtstack.flinkx.metadata.MetaDataCons;

/**
 * @author : kunni@dtstack.com
 * @date : 2020/6/9
 * @description : sql语句将"select *"替换为"select 具体属性"
 */

public class OracleMetaDataCons extends MetaDataCons {

    public static final String DRIVER_NAME = "oracle.jdbc.driver.OracleDriver";

    public static final String KEY_TABLE_TYPE = "tableType";

    public static final String SQL_QUERY_INDEX = "AND COLUMNS.TABLE_NAME IN (%s) ";
    public static final String SQL_QUERY_COLUMN = "AND COLUMNS.TABLE_NAME IN (%s) ";
    public static final String SQL_QUERY_PRIMARY_KEY = "AND COLUMNS.TABLE_NAME IN (%s) ";
    public static final String SQL_QUERY_TABLE_PROPERTIES = "AND TABLES.TABLE_NAME IN (%s) ";
    public static final String SQL_QUERY_TABLE_CREATE_TIME = "AND TABLES.TABLE_NAME IN (%s) ";

    /**
     * 使用ALL_系统表，降低权限要求
     */

    public static final String SQL_QUERY_INDEX_TOTAL = "SELECT COLUMNS.INDEX_NAME, COLUMNS.COLUMN_NAME, INDEXES.INDEX_TYPE, COLUMNS.TABLE_NAME  " +
            "FROM ALL_IND_COLUMNS COLUMNS  " +
            "LEFT JOIN ALL_INDEXES INDEXES  " +
            "ON COLUMNS.INDEX_NAME = INDEXES.INDEX_NAME  " +
            "AND COLUMNS.TABLE_NAME = INDEXES.TABLE_NAME  " +
            "AND COLUMNS.TABLE_OWNER = INDEXES.TABLE_OWNER  " +
            "WHERE COLUMNS.TABLE_OWNER = %s ";
    public static final String SQL_QUERY_COLUMN_TOTAL = "SELECT COLUMNS.COLUMN_NAME, COLUMNS.DATA_TYPE, COMMENTS.COMMENTS, COLUMNS.TABLE_NAME, " +
            "DATA_DEFAULT, NULLABLE, DATA_LENGTH  " +
            "FROM ALL_TAB_COLUMNS COLUMNS  " +
            "LEFT JOIN ALL_COL_COMMENTS COMMENTS  " +
            "ON COLUMNS.OWNER = COMMENTS.OWNER  " +
            "AND COLUMNS.TABLE_NAME = COMMENTS.TABLE_NAME  " +
            "AND COLUMNS.COLUMN_NAME = COMMENTS.COLUMN_NAME  " +
            "WHERE COLUMNS.OWNER = %s ";
    public static final String SQL_QUERY_TABLE_PROPERTIES_TOTAL = "SELECT TABLES.NUM_ROWS * TABLES.AVG_ROW_LEN AS TOTALSIZE, " +
            "COMMENTS.COMMENTS, COMMENTS.TABLE_TYPE, TABLES.NUM_ROWS, TABLES.TABLE_NAME " +
            "FROM ALL_TABLES TABLES " +
            "LEFT JOIN ALL_TAB_COMMENTS COMMENTS " +
            "ON TABLES.OWNER = COMMENTS.OWNER " +
            "AND TABLES.TABLE_NAME = COMMENTS.TABLE_NAME " +
            "WHERE TABLES.OWNER = %s ";
    public static final String SQL_QUERY_PRIMARY_KEY_TOTAL = "SELECT COLUMNS.TABLE_NAME, COLUMNS.COLUMN_NAME  " +
            "FROM ALL_CONS_COLUMNS COLUMNS, ALL_CONSTRAINTS CONS  " +
            "WHERE COLUMNS.CONSTRAINT_NAME = CONS.CONSTRAINT_NAME  " +
            "AND CONS.CONSTRAINT_TYPE = 'P'  " +
            "AND COLUMNS.OWNER = %s ";
    public static final String SQL_QUERY_TABLE_CREATE_TIME_TOTAL = "SELECT TABLES.TABLE_NAME, OBJS.CREATED " +
            "FROM ALL_TABLES TABLES " +
            "LEFT JOIN ALL_OBJECTS OBJS " +
            "ON TABLES.OWNER = OBJS.OWNER " +
            "AND TABLES.TABLE_NAME = OBJS.OBJECT_NAME " +
            "WHERE TABLES.OWNER = %s ";

    /**
     * 查询特定schema下的所有表
     * 排除嵌套表等特殊表
     */
    public static final String SQL_SHOW_TABLES = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = %s AND NESTED = 'NO' AND IOT_NAME IS NULL ";
}