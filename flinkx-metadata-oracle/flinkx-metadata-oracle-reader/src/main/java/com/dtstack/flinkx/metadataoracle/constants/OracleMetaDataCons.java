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

    public static final String KEY_ROWS = "rows";
    public static final String KEY_TABLE_TYPE = "tableType";
    public static final String KEY_CREATE_TIME = "createTime";
    public static final String KEY_TOTAL_SIZE = "totalSize";
    public static final String KEY_INDEX_COLUMN_NAME = "columnName";

    public static final String SQL_QUERY_INDEX = "SELECT COLUMNS.INDEX_NAME, COLUMNS.COLUMN_NAME, INDEXES.INDEX_TYPE, COLUMNS.TABLE_NAME " +
            "FROM DBA_IND_COLUMNS COLUMNS JOIN DBA_INDEXES INDEXES \n" +
            "ON COLUMNS.INDEX_NAME = INDEXES.INDEX_NAME AND COLUMNS.TABLE_NAME = INDEXES.TABLE_NAME AND COLUMNS.TABLE_OWNER = INDEXES.TABLE_OWNER \n" +
            "WHERE COLUMNS.TABLE_OWNER = %s AND COLUMNS.TABLE_NAME IN (%s) ";
    public static final String SQL_QUERY_COLUMN = "SELECT COLUMNS.COLUMN_NAME, COLUMNS.DATA_TYPE, COMMENTS.COMMENTS, COLUMNS.TABLE_NAME, DATA_DEFAULT, NULLABLE, DATA_LENGTH  " +
            "FROM DBA_TAB_COLUMNS COLUMNS JOIN DBA_COL_COMMENTS COMMENTS \n" +
            "ON COLUMNS.OWNER = COMMENTS.OWNER AND COLUMNS.TABLE_NAME = COMMENTS.TABLE_NAME AND COLUMNS.COLUMN_NAME = COMMENTS.COLUMN_NAME\n" +
            "WHERE COLUMNS.OWNER = %s AND COLUMNS.TABLE_NAME IN (%s) ";
    public static final String SQL_QUERY_PRIMARY_KEY = "SELECT a.table_name, a.column_name \n" +
            "FROM user_cons_columns a, user_constraints b \n" +
            "WHERE a.constraint_name = b.constraint_name \n" +
            "AND a.OWNER = %s AND b.constraint_type = 'P' AND a.table_name IN (%s) ";
    public static final String SQL_QUERY_TABLE_PROPERTIES = "SELECT TABLES.NUM_ROWS*TABLES.AVG_ROW_LEN AS TOTALSIZE, COMMENTS.COMMENTS, COMMENTS.TABLE_TYPE, OBJS.CREATED, TABLES.NUM_ROWS, TABLES.TABLE_NAME \n" +
            "FROM DBA_TABLES TABLES JOIN DBA_OBJECTS OBJS ON TABLES.OWNER = OBJS.OWNER AND TABLES.TABLE_NAME = OBJS.OBJECT_NAME\n" +
            "JOIN DBA_TAB_COMMENTS COMMENTS ON TABLES.OWNER = COMMENTS.OWNER AND TABLES.TABLE_NAME = COMMENTS.TABLE_NAME\n" +
            "WHERE TABLES.OWNER = %s AND TABLES.TABLE_NAME IN (%s)";

    public static final String SQL_QUERY_INDEX_TOTAL = "SELECT COLUMNS.INDEX_NAME, COLUMNS.COLUMN_NAME, INDEXES.INDEX_TYPE, COLUMNS.TABLE_NAME " +
            "FROM DBA_IND_COLUMNS COLUMNS JOIN DBA_INDEXES INDEXES \n" +
            "ON COLUMNS.INDEX_NAME = INDEXES.INDEX_NAME AND COLUMNS.TABLE_NAME = INDEXES.TABLE_NAME AND COLUMNS.TABLE_OWNER = INDEXES.TABLE_OWNER \n" +
            "WHERE COLUMNS.TABLE_OWNER = %s ";
    public static final String SQL_QUERY_COLUMN_TOTAL = "SELECT COLUMNS.COLUMN_NAME, COLUMNS.DATA_TYPE, COMMENTS.COMMENTS, COLUMNS.TABLE_NAME, DATA_DEFAULT, NULLABLE, DATA_LENGTH  " +
            "FROM DBA_TAB_COLUMNS COLUMNS JOIN DBA_COL_COMMENTS COMMENTS \n" +
            "ON COLUMNS.OWNER = COMMENTS.OWNER AND COLUMNS.TABLE_NAME = COMMENTS.TABLE_NAME AND COLUMNS.COLUMN_NAME = COMMENTS.COLUMN_NAME\n" +
            "WHERE COLUMNS.OWNER = %s ";
    public static final String SQL_QUERY_TABLE_PROPERTIES_TOTAL = "SELECT TABLES.NUM_ROWS*TABLES.AVG_ROW_LEN AS TOTALSIZE, COMMENTS.COMMENTS, COMMENTS.TABLE_TYPE, OBJS.CREATED, TABLES.NUM_ROWS, TABLES.TABLE_NAME \n" +
            "FROM DBA_TABLES TABLES JOIN DBA_OBJECTS OBJS ON TABLES.OWNER = OBJS.OWNER AND TABLES.TABLE_NAME = OBJS.OBJECT_NAME\n" +
            "JOIN DBA_TAB_COMMENTS COMMENTS ON TABLES.OWNER = COMMENTS.OWNER AND TABLES.TABLE_NAME = COMMENTS.TABLE_NAME\n" +
            "WHERE TABLES.OWNER = %s ";
    public static final String SQL_QUERY_PRIMARY_KEY_TOTAL = "SELECT a.table_name, a.column_name \n" +
            "FROM user_cons_columns a, user_constraints b \n" +
            "WHERE a.constraint_name = b.constraint_name \n" +
            "AND b.constraint_type = 'P' AND a.OWNER = %s ";

    /**
     * 使用ALL_TABLES，降低权限要求
     */
    public static final String SQL_SHOW_TABLES = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = %s AND NESTED = 'NO' AND IOT_NAME IS NULL ";
}