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

package com.dtstack.flinkx.hive.util;

import com.dtstack.flinkx.hive.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.hive.EStoreType.ORC;
import static com.dtstack.flinkx.hive.EStoreType.TEXT;

/**
 * @author toutian
 */
public class HiveUtil {

    private static Logger logger = LoggerFactory.getLogger(HiveUtil.class);

    public static final String LEFT_BRACKETS = "(";

    private static final String CREATE_PARTITION_TEMPLATE = "alter table %s add if not exists partition (%s)";
    private static final String CREATE_DIRTY_DATA_TABLE_TEMPLATE = "CREATE TABLE IF NOT EXISTS dirty_%s (event STRING, error STRING, created STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' LINES TERMINATED BY '\\n' STORED AS TEXTFILE";

    private static final String NO_SUCH_TABLE_EXCEPTION = "NoSuchTableException";

    private final List<String> tableExistException = Arrays.asList("TableExistsException", "AlreadyExistsException", "TableAlreadyExistsException");

    public final static String TABLE_COLUMN_KEY = "key";
    public final static String TABLE_COLUMN_TYPE = "type";
    public final static String PARTITION_TEMPLATE = "%s=%s";

    private HiveDbUtil.ConnectionInfo connectionInfo;

    enum HiveReleaseVersion{
        /**
         * apache hive 1.x
         */
        APACHE_1("apache", "1"),

        /**
         * apache hive 2.x
         */
        APACHE_2("apache", "2"),

        /**
         * cdh hive 1.x
         */
        CDH_1("cdh", "1"),

        /**
         * cdh hive 2.x
         */
        CDH_2("cdh", "2");

        private String name;

        private String version;

        HiveReleaseVersion(String name, String version) {
            this.name = name;
            this.version = version;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }
    }

    public HiveUtil() {
    }

    /**
     * 抛出异常,直接终止hive
     */
    public HiveUtil(HiveDbUtil.ConnectionInfo connectionInfo) {
        this.connectionInfo = connectionInfo;
    }

    public void createHiveTableWithTableInfo(TableInfo tableInfo) {
        Connection connection = null;
        try {
            connection = HiveDbUtil.getConnection(connectionInfo);
            createTable(connection, tableInfo);
            fillTableInfo(connection, tableInfo);
        } catch (Exception e) {
            logger.error("", e);
            throw e;
        } finally {
            HiveDbUtil.closeDbResources(null, null, connection);
        }
    }

    /**
     * 创建hive的分区
     */
    public void createPartition(TableInfo tableInfo, String partition) {
        Connection connection = null;
        try {
            connection = HiveDbUtil.getConnection(connectionInfo);
            String sql = String.format(CREATE_PARTITION_TEMPLATE, tableInfo.getTablePath(), partition);
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
    private void createTable(Connection connection, TableInfo tableInfo) {
        try {
            String sql = String.format(tableInfo.getCreateTableSql(), tableInfo.getTablePath());
            HiveDbUtil.executeSqlWithoutResultSet(connectionInfo, connection, sql);
        } catch (Exception e) {
            if (!isTableExistsException(e.getMessage())) {
                logger.error("create table happens error:", e);
                throw new RuntimeException("create table happens error", e);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not need create table:{}, it's already exist", tableInfo.getTablePath());
                }
            }
        }
    }

    private boolean isTableExistsException(String message){
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

    private void fillTableInfo(Connection connection, TableInfo tableInfo) {
        try {
            HiveReleaseVersion hiveVersion = getHiveVersion(connection);
            AbstractHiveMetadataParser metadataParser = getMetadataParser(hiveVersion);

            List<Map<String, Object>> result = HiveDbUtil.executeQuery(connection, "desc formatted " + tableInfo.getTablePath());
            metadataParser.fillTableInfo(tableInfo, result);
        } catch (Exception e) {
            logger.error("{}", e);
            if (e.getMessage().contains(NO_SUCH_TABLE_EXCEPTION)) {
                throw new RuntimeException(String.format("表%s不存在", tableInfo.getTablePath()));
            } else {
                throw e;
            }
        }
    }

    private AbstractHiveMetadataParser getMetadataParser(HiveReleaseVersion hiveVersion){
        if (HiveReleaseVersion.APACHE_2.equals(hiveVersion) || HiveReleaseVersion.APACHE_1.equals(hiveVersion)) {
            return new Apache2MetadataParser();
        } else {
            return new Cdh2HiveMetadataParser();
        }
    }

    public HiveReleaseVersion getHiveVersion(Connection connection){
        HiveReleaseVersion version = HiveReleaseVersion.APACHE_2;
        try (ResultSet resultSet = connection.createStatement().executeQuery("select version()")) {
            if (resultSet.next()) {
                String versionMsg = resultSet.getString(1);
                if (versionMsg.contains(HiveReleaseVersion.CDH_1.getName())){
                    // 结果示例：2.1.1-cdh6.3.1 re8d55f408b4f9aa2648bc9e34a8f802d53d6aab3
                    if (versionMsg.startsWith(HiveReleaseVersion.CDH_2.getVersion())) {
                        version = HiveReleaseVersion.CDH_2;
                    } else if(versionMsg.startsWith(HiveReleaseVersion.CDH_1.getVersion())){
                        version = HiveReleaseVersion.CDH_1;
                    }
                } else {
                    // FIXME spark thrift server不支持 version()函数，所以使用默认的版本
                }
            }
        } catch (Exception ignore) {
        }

        return version;
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

    public static String getHiveColumnType(String originType) {
        originType = originType.trim();
        int indexOfBrackets = originType.indexOf(LEFT_BRACKETS);
        if (indexOfBrackets > -1) {
            String params = originType.substring(indexOfBrackets);
            int index = params.indexOf(",");
            int right = Integer.parseInt(params.substring(index+1, params.length()-1).trim());
            if(right == 0){
                int left = Integer.parseInt(params.substring(1, index).trim());
                if(left <= 4){
                    return "SMALLINT";
                }else if(left <= 9){
                    return "INT";
                }else if(left <= 18){
                    return "BIGINT";
                }
            }
            return "DECIMAL" + params;
        } else {
            return convertType(originType);
        }
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
            case "FLOAT8":
                type = "FLOAT";
                break;
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
}
