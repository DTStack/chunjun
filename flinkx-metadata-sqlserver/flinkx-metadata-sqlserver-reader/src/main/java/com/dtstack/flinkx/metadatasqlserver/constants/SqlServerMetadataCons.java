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

package com.dtstack.flinkx.metadatasqlserver.constants;

import com.dtstack.flinkx.metadata.MetaDataCons;

/**
 * @author : kunni@dtstack.com
 * @date : 2020/08/06
 */

public class SqlServerMetadataCons extends MetaDataCons {

    public static final String DRIVER_NAME = "net.sourceforge.jtds.jdbc.Driver";

    public static final String KEY_CREATE_TIME = "createTime";
    public static final String KEY_ROWS = "rows";
    public static final String KEY_TOTAL_SIZE = "totalSize";
    public static final String KEY_PARTITION_COLUMN = "partitionColumn";
    public static final String KEY_COLUMN_NAME = "columnName";
    public static final String KEY_FILE_GROUP_NAME = "fileGroupName";

    public static final String SQL_SWITCH_DATABASE = "USE %s";
    /**
     * 拼接成schema.table
     */
    public static final String SQL_SHOW_TABLES = "SELECT OBJECT_SCHEMA_NAME(object_id, DB_ID()) + '.' + name as name FROM sys.tables";

    public static final String SQL_SHOW_TABLE_PROPERTIES = "SELECT a.crdate, b.rows, rtrim(8*dpages) used \n" +
            "FROM sysobjects AS a INNER JOIN sysindexes AS b ON a.id = b.id \n" +
            "WHERE (a.type = 'u') AND (b.indid IN (0, 1)) and a.name = %s and OBJECT_SCHEMA_NAME(a.id, DB_ID()) = %s";

    public static final String SQL_SHOW_TABLE_COLUMN = "SELECT B.name AS name, TY.name as type, C.value AS comment \n" +
            "FROM sys.tables A INNER JOIN sys.columns B ON B.object_id = A.object_id \n" +
            "INNER JOIN sys.types TY ON B.system_type_id = TY.system_type_id \n" +
            "LEFT JOIN sys.extended_properties C ON C.major_id = B.object_id AND C.minor_id = B.column_id \n" +
            "WHERE A.name = %s and OBJECT_SCHEMA_NAME(A.object_id, DB_ID())=%s";

    public static final String SQL_SHOW_TABLE_INDEX = "SELECT a.name, d.name as columnName, type_desc as type \n" +
            "FROM sys.indexes a JOIN sysindexkeys b ON a.object_id=b.id AND a.index_id=b.indid \n" +
            "JOIN sysobjects c ON b.id=c.id JOIN syscolumns d ON b.id=d.id AND b.colid=d.colid \n" +
            "WHERE c.name=%s and OBJECT_SCHEMA_NAME(A.object_id, DB_ID())=%s \n" +
            "AND  a.index_id  NOT IN(0,255)";

    public static final String SQL_SHOW_PARTITION_COLUMN = "SELECT d.name as columnName \n" +
            "FROM sys.indexes a JOIN sysindexkeys b ON a.object_id=b.id AND a.index_id=b.indid \n" +
            "JOIN sysobjects c ON b.id=c.id JOIN syscolumns d ON b.id=d.id AND b.colid=d.colid \n" +
            "WHERE c.name=%s and OBJECT_SCHEMA_NAME(A.object_id, DB_ID())=%s \n" +
            "AND  a.type in (0, 1)";

    public static final String SQL_SHOW_PARTITION = "select ps.name, p.rows, pf.create_date, ds2.name as filegroup \n" +
            "from sys.indexes i join sys.partition_schemes ps on i.data_space_id = ps.data_space_id \n" +
            "join sys.destination_data_spaces dds on ps.data_space_id = dds.partition_scheme_id \n" +
            "join sys.data_spaces ds2 on dds.data_space_id = ds2.data_space_id \n" +
            "join sys.partitions p on dds.destination_id = p.partition_number \n" +
            "and p.object_id = i.object_id and p.index_id = i.index_id \n" +
            "join sys.partition_functions pf on ps.function_id = pf.function_id \n" +
            "WHERE i.object_id = object_id(%s) and OBJECT_SCHEMA_NAME(i.object_id, DB_ID())=%s \n" +
            "and i.index_id in (0, 1)";

}
