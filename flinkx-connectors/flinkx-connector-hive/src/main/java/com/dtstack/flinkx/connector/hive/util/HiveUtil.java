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
package com.dtstack.flinkx.connector.hive.util;

import com.dtstack.flinkx.connector.hdfs.enums.FileType;
import com.dtstack.flinkx.connector.hive.entity.ConnectionInfo;
import com.dtstack.flinkx.connector.hive.entity.TableInfo;
import com.dtstack.flinkx.connector.hive.enums.HiveReleaseVersion;
import com.dtstack.flinkx.connector.hive.parser.AbstractHiveMetadataParser;
import com.dtstack.flinkx.connector.hive.parser.Apache2MetadataParser;
import com.dtstack.flinkx.connector.hive.parser.Cdh2HiveMetadataParser;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.BytesColumn;
import com.dtstack.flinkx.element.column.NullColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.util.GsonUtil;

import org.apache.flink.api.common.cache.DistributedCache;

import com.google.common.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Date: 2021/06/22 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HiveUtil {
    public static final String TABLE_COLUMN_KEY = "key";
    public static final String TABLE_COLUMN_TYPE = "type";
    public static final String PARTITION_TEMPLATE = "%s=%s";
    private static final Logger logger = LoggerFactory.getLogger(HiveUtil.class);
    private static final String CREATE_PARTITION_TEMPLATE =
            "alter table %s add if not exists partition (%s)";
    private static final String NO_SUCH_TABLE_EXCEPTION = "NoSuchTableException";
    private static final List<String> tableExistException =
            Arrays.asList(
                    "TableExistsException",
                    "AlreadyExistsException",
                    "TableAlreadyExistsException");

    public static void createHiveTableWithTableInfo(
            TableInfo tableInfo,
            String schema,
            ConnectionInfo connectionInfo,
            DistributedCache distributedCache) {
        Connection connection = null;
        try {
            connection = HiveDbUtil.getConnection(connectionInfo, distributedCache);
            if (StringUtils.isNotBlank(schema)) {
                HiveDbUtil.executeSqlWithoutResultSet(connectionInfo, connection, "use " + schema);
            }
            createTable(connection, tableInfo, connectionInfo);
            fillTableInfo(connection, tableInfo);
        } catch (Exception e) {
            logger.error("", e);
            throw e;
        } finally {
            HiveDbUtil.closeDbResources(null, null, connection);
        }
    }

    /** 创建hive的分区 */
    public static void createPartition(
            TableInfo tableInfo,
            String schema,
            String partition,
            ConnectionInfo connectionInfo,
            DistributedCache distributedCache) {
        Connection connection = null;
        try {
            connection = HiveDbUtil.getConnection(connectionInfo, distributedCache);
            if (StringUtils.isNotBlank(schema)) {
                HiveDbUtil.executeSqlWithoutResultSet(connectionInfo, connection, "use " + schema);
            }
            String sql =
                    String.format(CREATE_PARTITION_TEMPLATE, tableInfo.getTablePath(), partition);
            HiveDbUtil.executeSqlWithoutResultSet(connectionInfo, connection, sql);
        } catch (Exception e) {
            logger.error("", e);
            throw e;
        } finally {
            HiveDbUtil.closeDbResources(null, null, connection);
        }
    }

    /**
     * 表如果存在不要删除之前的表因为可能是用户的表，所以也不需要再创建，也不用 throw exception，暂时只有日志
     *
     * @param connection
     * @param tableInfo
     */
    private static void createTable(
            Connection connection, TableInfo tableInfo, ConnectionInfo connectionInfo) {
        try {
            String sql = String.format(tableInfo.getCreateTableSql(), tableInfo.getTablePath());
            HiveDbUtil.executeSqlWithoutResultSet(connectionInfo, connection, sql);
        } catch (Exception e) {
            if (!isTableExistsException(e.getMessage())) {
                logger.error("create table happens error:", e);
                throw new RuntimeException("create table happens error", e);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Not need create table:{}, it's already exist",
                            tableInfo.getTablePath());
                }
            }
        }
    }

    private static boolean isTableExistsException(String message) {
        if (message == null) {
            return false;
        }

        for (String msg : tableExistException) {
            if (message.contains(msg)) {
                return true;
            }
        }

        return false;
    }

    private static void fillTableInfo(Connection connection, TableInfo tableInfo) {
        try {
            HiveReleaseVersion hiveVersion = getHiveVersion(connection);
            AbstractHiveMetadataParser metadataParser = getMetadataParser(hiveVersion);

            List<Map<String, Object>> result =
                    HiveDbUtil.executeQuery(
                            connection, "desc formatted " + tableInfo.getTablePath());
            metadataParser.fillTableInfo(tableInfo, result);
        } catch (Exception e) {
            if (e.getMessage().contains(NO_SUCH_TABLE_EXCEPTION)) {
                throw new FlinkxRuntimeException(String.format("表%s不存在", tableInfo.getTablePath()));
            } else {
                throw e;
            }
        }
    }

    private static AbstractHiveMetadataParser getMetadataParser(HiveReleaseVersion hiveVersion) {
        if (HiveReleaseVersion.APACHE_2.equals(hiveVersion)
                || HiveReleaseVersion.APACHE_1.equals(hiveVersion)) {
            return new Apache2MetadataParser();
        } else {
            return new Cdh2HiveMetadataParser();
        }
    }

    public static HiveReleaseVersion getHiveVersion(Connection connection) {
        HiveReleaseVersion version = HiveReleaseVersion.APACHE_2;
        try (ResultSet resultSet = connection.createStatement().executeQuery("select version()")) {
            if (resultSet.next()) {
                String versionMsg = resultSet.getString(1);
                if (versionMsg.contains(HiveReleaseVersion.CDH_1.getName())) {
                    // 结果示例：2.1.1-cdh6.3.1 re8d55f408b4f9aa2648bc9e34a8f802d53d6aab3
                    if (versionMsg.startsWith(HiveReleaseVersion.CDH_2.getVersion())) {
                        version = HiveReleaseVersion.CDH_2;
                    } else if (versionMsg.startsWith(HiveReleaseVersion.CDH_1.getVersion())) {
                        version = HiveReleaseVersion.CDH_1;
                    }
                } else {
                    // spark thrift server不支持 version()函数，所以使用默认的版本
                }
            }
        } catch (Exception ignore) {
        }

        return version;
    }

    public static String getCreateTableHql(TableInfo tableInfo) {
        // 不要使用create table if not exist，可能以后会在业务逻辑中判断表是否已经存在
        StringBuilder sql = new StringBuilder(256);
        sql.append("CREATE TABLE %s (");
        for (int i = 0; i < tableInfo.getColumnNameList().size(); i++) {
            sql.append(
                    String.format(
                            "`%s` %s",
                            tableInfo.getColumnNameList().get(i),
                            tableInfo.getColumnTypeList().get(i)));
            if (i != tableInfo.getColumnNameList().size() - 1) {
                sql.append(",");
            }
        }
        sql.append(") ");
        if (!tableInfo.getPartitionList().isEmpty()) {
            sql.append(" PARTITIONED BY (");
            for (String partitionField : tableInfo.getPartitionList()) {
                sql.append(String.format("`%s` string", partitionField));
            }
            sql.append(") ");
        }
        if (FileType.TEXT.name().equalsIgnoreCase(tableInfo.getStore())) {
            sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '");
            sql.append(tableInfo.getDelimiter());
            sql.append("' LINES TERMINATED BY '\\n' STORED AS TEXTFILE ");
        } else if (FileType.ORC.name().equalsIgnoreCase(tableInfo.getStore())) {
            sql.append(" STORED AS ORC ");
        } else {
            sql.append(" STORED AS PARQUET ");
        }
        return sql.toString();
    }

    /**
     * 分表的映射关系 distributeTableMapping 的数据结构为<tableName,groupName>
     * tableInfos的数据结构为<groupName,TableInfo>
     */
    public static Map<String, String> formatHiveDistributeInfo(String distributeTable) {
        Map<String, String> distributeTableMapping = new HashMap<>(32);
        if (StringUtils.isNotBlank(distributeTable)) {
            Map<String, List<String>> distributeTableMap =
                    GsonUtil.GSON.fromJson(
                            distributeTable,
                            new TypeToken<TreeMap<String, List<String>>>() {}.getType());
            for (Map.Entry<String, List<String>> entry : distributeTableMap.entrySet()) {
                String groupName = entry.getKey();
                List<String> groupTables = entry.getValue();
                for (String tableName : groupTables) {
                    distributeTableMapping.put(tableName, groupName);
                }
            }
        }
        return distributeTableMapping;
    }

    public static Map<String, TableInfo> formatHiveTableInfo(
            String tablesColumn, String partition, String fieldDelimiter, String fileType) {
        Map<String, TableInfo> tableInfos = new HashMap<>(16);
        if (StringUtils.isNotEmpty(tablesColumn)) {
            Map<String, List<Map<String, Object>>> tableColumnMap =
                    GsonUtil.GSON.fromJson(
                            tablesColumn,
                            new com.google.gson.reflect.TypeToken<
                                    TreeMap<String, List<Map<String, Object>>>>() {}.getType());
            for (Map.Entry<String, List<Map<String, Object>>> entry : tableColumnMap.entrySet()) {
                String tableName = entry.getKey();
                List<Map<String, Object>> tableColumns = entry.getValue();
                TableInfo tableInfo = new TableInfo(tableColumns.size());
                tableInfo.addPartition(partition);
                tableInfo.setDelimiter(fieldDelimiter);
                tableInfo.setStore(fileType);
                tableInfo.setTableName(tableName);
                for (Map<String, Object> column : tableColumns) {
                    tableInfo.addColumnAndType(
                            MapUtils.getString(column, HiveUtil.TABLE_COLUMN_KEY),
                            convertType(MapUtils.getString(column, HiveUtil.TABLE_COLUMN_TYPE)));
                }
                String createTableSql = HiveUtil.getCreateTableHql(tableInfo);
                tableInfo.setCreateTableSql(createTableSql);

                tableInfos.put(tableName, tableInfo);
            }
        }
        return tableInfos;
    }

    private static String convertType(String type) {
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
            case "NUMERIC":
            case "NUMBER":
            case "BIGINT":
                type = "BIGINT";
                break;
            case "REAL":
            case "FLOAT":
            case "FLOAT2":
            case "FLOAT4":
                type = "FLOAT";
                break;
            case "FLOAT8":
            case "DOUBLE":
            case "BINARY_DOUBLE":
                type = "DOUBLE";
                break;
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

    public static AbstractBaseColumn parseDataFromMap(Object data) {
        if (data == null) {
            return new NullColumn();
        } else if (data instanceof String) {
            return new StringColumn((String) data);
        } else if (data instanceof Character) {
            return new StringColumn(String.valueOf(data));
        } else if (data instanceof Boolean) {
            return new BooleanColumn((Boolean) data);
        } else if (data instanceof Byte) {
            return new BigDecimalColumn((Byte) data);
        } else if (data instanceof Short) {
            return new BigDecimalColumn((Short) data);
        } else if (data instanceof Integer) {
            return new BigDecimalColumn((Integer) data);
        } else if (data instanceof Long) {
            return new BigDecimalColumn((Long) data);
        } else if (data instanceof BigInteger) {
            return new BigDecimalColumn((BigInteger) data);
        } else if (data instanceof Float) {
            return new BigDecimalColumn((Float) data);
        } else if (data instanceof Double) {
            return new BigDecimalColumn((Double) data);
        } else if (data instanceof BigDecimal) {
            return new BigDecimalColumn((BigDecimal) data);
        } else if (data instanceof Timestamp) {
            return new TimestampColumn((Timestamp) data);
        } else if (data instanceof Date) {
            return new TimestampColumn((Date) data);
        } else if (data instanceof byte[]) {
            return new BytesColumn((byte[]) data);
        } else {
            logger.debug("unknown type: [{}], data: [{}]", data.getClass(), data);
            return new StringColumn(GsonUtil.GSON.toJson(data));
        }
    }
}
