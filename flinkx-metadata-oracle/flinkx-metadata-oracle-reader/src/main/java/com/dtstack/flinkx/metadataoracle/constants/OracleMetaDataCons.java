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

import com.dtstack.metadata.rdb.core.constants.RdbCons;

/**
 * @author : kunni@dtstack.com
 * @date : 2020/6/9
 * @description : sql语句将"select *"替换为"select 具体属性"
 */

public class OracleMetaDataCons extends RdbCons {

    public static final String DRIVER_NAME = "oracle.jdbc.driver.OracleDriver";

    public static final String KEY_NUMBER = "NUMBER";

    public static final String NUMBER_PRECISION = "NUMBER(%s,%s)";

    public static final String KEY_MAX_NUMBER = "127";

    public static final String KEY_PRIMARY_KEY = "primaryKey";

    public static final String KEY_CREATE_TIME = "createTime";

    public static final String KEY_PARTITION_KEY = "partitionKey";

    public static final int MAX_TABLE_SIZE = 2;


    /**
     * 通过in语法，减少内存占用
     */
    public static final String SQL_QUERY_INDEX = "AND COLUMNS.TABLE_NAME IN (%s) ";
    public static final String SQL_QUERY_COLUMN = "AND COLUMNS.TABLE_NAME IN (%s) ";
    public static final String SQL_QUERY_PRIMARY_KEY = "AND COLUMNS.TABLE_NAME IN (%s) ";
    public static final String SQL_QUERY_TABLE_PROPERTIES = "AND TABLES.TABLE_NAME IN (%s) ";
    public static final String SQL_QUERY_TABLE_CREATE_TIME = "AND TABLES.TABLE_NAME IN (%s) ";
    public static final String SQL_QUERY_TABLE_PARTITION_KEY = "AND NAME IN (%s) ";

    /**
     * 查询索引信息
     */
    public static final String SQL_QUERY_INDEX_TOTAL =
            "SELECT COLUMNS.INDEX_NAME, COLUMNS.COLUMN_NAME, INDEXES.INDEX_TYPE, COLUMNS.TABLE_NAME  " +
            "FROM ALL_IND_COLUMNS COLUMNS  " +
            "LEFT JOIN ALL_INDEXES INDEXES  " +
            "ON COLUMNS.INDEX_NAME = INDEXES.INDEX_NAME  " +
            "AND COLUMNS.TABLE_NAME = INDEXES.TABLE_NAME  " +
            "AND COLUMNS.TABLE_OWNER = INDEXES.TABLE_OWNER  " +
            "WHERE COLUMNS.TABLE_OWNER = %s ";
    /**
     * 查询列的基本信息
     * DATA_LENGTH列的长度、DATA_PRECISION小数精度、DATA_SCALE小数点右边数字
     */
    public static final String SQL_QUERY_COLUMN_TOTAL =
            "SELECT COLUMNS.COLUMN_NAME, COLUMNS.DATA_TYPE, COMMENTS.COMMENTS, COLUMNS.TABLE_NAME, " +
            "DATA_DEFAULT, NULLABLE, DATA_LENGTH,  " +
            "COLUMNS.COLUMN_ID, COLUMNS.DATA_PRECISION, COLUMNS.DATA_SCALE "+
            "FROM ALL_TAB_COLUMNS COLUMNS  " +
            "LEFT JOIN ALL_COL_COMMENTS COMMENTS  " +
            "ON COLUMNS.OWNER = COMMENTS.OWNER  " +
            "AND COLUMNS.TABLE_NAME = COMMENTS.TABLE_NAME  " +
            "AND COLUMNS.COLUMN_NAME = COMMENTS.COLUMN_NAME  " +
            "WHERE COLUMNS.OWNER = %s ";
    /**
     * 查询表的基本信息
     */
    public static final String SQL_QUERY_TABLE_PROPERTIES_TOTAL =
            "SELECT TABLES.NUM_ROWS * TABLES.AVG_ROW_LEN AS TOTALSIZE, " +
            "COMMENTS.COMMENTS, COMMENTS.TABLE_TYPE, TABLES.NUM_ROWS, TABLES.TABLE_NAME " +
            "FROM ALL_TABLES TABLES " +
            "LEFT JOIN ALL_TAB_COMMENTS COMMENTS " +
            "ON TABLES.OWNER = COMMENTS.OWNER " +
            "AND TABLES.TABLE_NAME = COMMENTS.TABLE_NAME " +
            "WHERE TABLES.OWNER = %s ";
    /**
     * 查询主键信息
     */
    public static final String SQL_QUERY_PRIMARY_KEY_TOTAL =
            "SELECT COLUMNS.TABLE_NAME, COLUMNS.COLUMN_NAME  " +
            "FROM ALL_CONS_COLUMNS COLUMNS, ALL_CONSTRAINTS CONS  " +
            "WHERE COLUMNS.CONSTRAINT_NAME = CONS.CONSTRAINT_NAME  " +
            "AND CONS.CONSTRAINT_TYPE = 'P'  " +
            "AND COLUMNS.OWNER = %s ";
    /**
     * 查询创建时间信息
     */
    public static final String SQL_QUERY_TABLE_CREATE_TIME_TOTAL =
            "SELECT TABLES.TABLE_NAME, OBJS.CREATED " +
            "FROM ALL_TABLES TABLES " +
            "LEFT JOIN ALL_OBJECTS OBJS " +
            "ON TABLES.OWNER = OBJS.OWNER " +
            "AND TABLES.TABLE_NAME = OBJS.OBJECT_NAME " +
            "WHERE TABLES.OWNER = %s ";

    /**
     * 查询分区列信息
     */
    public static final String SQL_PARTITION_KEY =
            "SELECT NAME, COLUMN_NAME FROM ALL_PART_KEY_COLUMNS " +
            "WHERE OBJECT_TYPE = 'TABLE' AND  OWNER=%s ";

    /**
     * 查询特定schema下的所有表
     * 排除嵌套表等特殊表
     */
    public static final String SQL_SHOW_TABLES =
            "SELECT TABLE_NAME FROM ALL_TABLES " +
            "WHERE OWNER = %s AND NESTED = 'NO' AND IOT_NAME IS NULL ";
}