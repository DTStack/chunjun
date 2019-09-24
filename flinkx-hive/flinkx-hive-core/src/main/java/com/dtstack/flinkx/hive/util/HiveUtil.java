/**
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

package com.dtstack.flinkx.hive.util;

import com.dtstack.flinkx.hive.TableInfo;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.dtstack.flinkx.hive.EStoreType.*;
import static com.dtstack.flinkx.hive.EWriteModeType.OVERWRITE;

/**
 * @author toutian
 */
public class HiveUtil {

    private static Logger logger = LoggerFactory.getLogger(HiveUtil.class);


    private static final String PATTERN_STR = "Storage\\(Location: (.*), InputFormat: (.*), OutputFormat: (.*) Serde: (.*) Properties: \\[(.*)\\]";
    private static final Pattern PATTERN = Pattern.compile(PATTERN_STR);
    private static final String CREATE_PARTITION_TEMPLATE = "alter table %s add if not exists partition (%s)";
    private static final Pattern DELIMITER_PATTERN = Pattern.compile("field\\.delim=(.*), ");
    private static final String CREATE_DIRTY_DATA_TABLE_TEMPLATE = "CREATE TABLE IF NOT EXISTS dirty_%s (event STRING, error STRING, created STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\\n' STORED AS TEXTFILE";
    private static final String TEXT_FORMAT = "TextOutputFormat";

    private static final String ORC_FORMAT = "OrcOutputFormat";
    private static final String PARQUET_FORMAT = "MapredParquetOutputFormat";
    private static final String NoSuchTableException = "NoSuchTableException";
    private static final String TableExistException = "TableExistsException";
    private static final String TableAlreadyExistsException = "TableAlreadyExistsException";

    public final static String TABLE_COLUMN_KEY = "key";
    public final static String TABLE_COLUMN_TYPE = "type";
    public final static String PARTITION_TEMPLATE = "%s=%s";

    private String writeMode;
    private DBUtil.ConnectionInfo connectionInfo;

    /**
     * 抛出异常,直接终止hive
     */
    public HiveUtil(DBUtil.ConnectionInfo connectionInfo, String writeMode) {
        this.connectionInfo = connectionInfo;
        this.writeMode = writeMode;
    }

    public void createHiveTableWithTableInfo(TableInfo tableInfo) {
        Connection connection = null;
        try {
            connection = DBUtil.getConnection(connectionInfo);
            createTable(connection, tableInfo);
            fillTableInfo(connection, tableInfo);
        } catch (Exception e) {
            logger.error("", e);
            throw e;
        } finally {
            DBUtil.closeDBResources(null, null, connection);
        }
    }

    /**
     * 创建hive的分区
     */
    public void createPartition(TableInfo tableInfo, String partition) {
        Connection connection = null;
        try {
            connection = DBUtil.getConnection(connectionInfo);
            String sql = String.format(CREATE_PARTITION_TEMPLATE, tableInfo.getTablePath(), partition);
            DBUtil.executeSqlWithoutResultSet(connection, sql);
        } catch (Exception e) {
            logger.error("", e);
            throw e;
        } finally {
            DBUtil.closeDBResources(null, null, connection);
        }
    }

    /**
     * 表如果存在不要删除之前的表因为可能是用户的表，所以也不需要再创建，也不用 throw exception，暂时只有日志
     *
     * @param connection
     * @param tableInfo
     */
    private void createTable(Connection connection, TableInfo tableInfo) {
        boolean overwrite = OVERWRITE.name().equalsIgnoreCase(writeMode);
        if (overwrite) {
            DBUtil.executeSqlWithoutResultSet(connection, String.format("DROP TABLE IF EXISTS %s", tableInfo.getTablePath()));
        }
        try {
            String sql = String.format(tableInfo.getCreateTableSql(), tableInfo.getTablePath());
            DBUtil.executeSqlWithoutResultSet(connection, sql);
        } catch (Exception e) {
            if (overwrite || !e.getMessage().contains(TableExistException) && !e.getMessage().contains(TableAlreadyExistsException)) {
                logger.error("create table happens error:", e);
                throw new RuntimeException("create table happens error", e);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not need create table:{}, it's already exist", tableInfo.getTablePath());
                }
            }
        }
    }

    private void fillTableInfo(Connection connection, TableInfo tableInfo) {
        try {
            List<Map<String, Object>> result = DBUtil.executeQuery(connection, "desc extended " + tableInfo.getTablePath());
            Iterator<Map<String, Object>> iter = result.iterator();
            String colName;
            String detail;
            while (iter.hasNext()) {
                Map<String, Object> row = iter.next();
                colName = (String) row.get("col_name");
                detail = (String) row.get("data_type");
                if (colName.equals("# Detailed Table Information")) {
                    if (detail != null) {
                        detail = detail.replaceAll("\n", " ");
                        Matcher matcher = PATTERN.matcher(detail);
                        if (matcher.find()) {
                            tableInfo.setPath(matcher.group(1));
                            if (matcher.group(3).contains(TEXT_FORMAT)) {
                                tableInfo.setStore(TEXT.name());
                                Matcher delimiterMatcher = DELIMITER_PATTERN.matcher(matcher.group(5));
                                if (delimiterMatcher.find()) {
                                    tableInfo.setDelimiter(delimiterMatcher.group(1));
                                }
                            } else if (matcher.group(3).contains(ORC_FORMAT)) {
                                tableInfo.setStore(ORC.name());
                            } else if (matcher.group(3).contains(PARQUET_FORMAT)) {
                                tableInfo.setStore(PARQUET.name());
                            }else {
                                throw new RuntimeException("Unsupported fileType:" + matcher.group(3));
                            }
                        }
                    }
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("{}", e);
            if (e.getMessage().contains(NoSuchTableException)) {
                throw new RuntimeException(String.format("表%s不存在", tableInfo.getTablePath()));
            } else {
                throw e;
            }
        }
    }


    public static String getCreateTableHql(TableInfo tableInfo) {
        //不要使用create table if not exist，可能以后会在业务逻辑中判断表是否已经存在
        StringBuilder fieldsb = new StringBuilder("CREATE TABLE %s (");
        for (int i = 0; i < tableInfo.getColumns().size(); i++) {
            fieldsb.append(String.format("`%s` %s", tableInfo.getColumns().get(i), tableInfo.getColumnTypes().get(i)));
            if (i != tableInfo.getColumns().size() - 1) {
                fieldsb.append(",");
            }
        }
        fieldsb.append(") ");
        if (!tableInfo.getPartitions().isEmpty()) {
            fieldsb.append(" PARTITIONED BY (");
            for (String partitionField : tableInfo.getPartitions()) {
                fieldsb.append(String.format("`%s` string", partitionField));
            }
            fieldsb.append(") ");
        }
        if (TEXT.name().equalsIgnoreCase(tableInfo.getStore())) {
            fieldsb.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '");
            fieldsb.append(tableInfo.getDelimiter());
            fieldsb.append("' LINES TERMINATED BY '\\n' STORED AS TEXTFILE ");
        } else if(ORC.name().equalsIgnoreCase(tableInfo.getStore())) {
            fieldsb.append(" STORED AS ORC ");
        }else{
            fieldsb.append(" STORED AS PARQUET ");
        }
        return fieldsb.toString();
    }

    public static String convertType(String type) {
        switch (type.toUpperCase()) {
            case "BIT":
            case "TINYINT":
                type = "TINYINT";
                break;
            case "SMALLINT":
                type = "SMALLINT";
                break;
            case "INT":
            case "MEDIUMINT":
            case "INTEGER":
            case "YEAR":
            case "INT2":
            case "INT4":
            case "INT8":
                type = "INT";
                break;
            case "BIGINT":
                type = "BIGINT";
                break;
            case "REAL":
            case "FLOAT":
            case "FLOAT2":
            case "FLOAT4":
            case "FLOAT8":
                type = "FLOAT";
                break;
            case "DOUBLE":
            case "BINARY_DOUBLE":
                type = "DOUBLE";
                break;
            case "NUMERIC":
            case "NUMBER":
            case "DECIMAL":
                type = "DECIMAL";
                break;
            case "STRING":
            case "VARCHAR":
            case "VARCHAR2":
            case "CHAR":
            case "CHARACTER":
            case "NCHAR":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "LONGVARCHAR":
            case "LONGNVARCHAR":
            case "NVARCHAR":
            case "NVARCHAR2":
                type = "STRING";
                break;
            case "BINARY":
                type = "BINARY";
                break;
            case "BOOLEAN":
                type = "BOOLEAN";
                break;
            case "DATE":
                type = "DATE";
                break;
            case "TIMESTAMP":
                type = "TIMESTAMP";
                break;
            default:
                type = "STRING";
        }
        return type;
    }

    public static ObjectInspector columnTypeToObjectInspetor(String columnType) {
        ObjectInspector objectInspector;
        switch (columnType.toUpperCase()) {
            case "TINYINT":
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Byte.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case "SMALLINT":
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Short.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case "INT":
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case "BIGINT":
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case "FLOAT":
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case "DOUBLE":
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case "DECIMAL":
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(HiveDecimalWritable.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case "TIMESTAMP":
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Timestamp.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case "DATE":
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Date.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case "BOOLEAN":
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case "BINARY":
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(BytesWritable.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            default:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        }
        return objectInspector;
    }
}
