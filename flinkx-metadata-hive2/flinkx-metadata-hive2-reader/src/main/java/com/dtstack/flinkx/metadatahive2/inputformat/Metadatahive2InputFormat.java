/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.metadatahive2.inputformat;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;
import com.dtstack.flinkx.metadatahive2.constants.HiveDbUtil;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_INDEX_COMMENT;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TABLE_COMMENT;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_COLUMN;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_COLUMN_COMMENT;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_COLUMN_DATA_TYPE;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_COLUMN_INDEX;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_COLUMN_NAME;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_COLUMN_TYPE;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_COL_CREATETIME;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_COL_CREATE_TIME;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_COL_LASTACCESSTIME;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_COL_LAST_ACCESS_TIME;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_COL_LOCATION;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_COL_NAME;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_COL_OUTPUTFORMAT;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_COL_TABLE_PARAMETERS;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_CREATETIME;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_LASTACCESSTIME;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_LOCATION;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_NAME;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_PARTITIONS;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_PARTITION_COLUMNS;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_RESULTSET_COL_NAME;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_RESULTSET_COMMENT;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_RESULTSET_DATA_TYPE;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_STORED_TYPE;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_TABLE_PROPERTIES;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_TOTALSIZE;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_TRANSIENT_LASTDDLTIME;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.KEY_VALUE;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.ORC_FORMAT;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.PARQUET_FORMAT;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.PARTITION_INFORMATION;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.SQL_QUERY_DATA;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.SQL_SHOW_PARTITIONS;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.SQL_SHOW_TABLES;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.SQL_SWITCH_DATABASE;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.TABLE_INFORMATION;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.TEXT_FORMAT;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.TYPE_ORC;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.TYPE_PARQUET;
import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.TYPE_TEXT;

/**
 * @author : tiezhu
 * @date : 2020/3/9
 */
public class Metadatahive2InputFormat extends BaseMetadataInputFormat {

    private static final long serialVersionUID = 1L;

    protected Map<String, Object> hadoopConfig;

    String paraFirst = KEY_COL_NAME;
    String paraSecond = KEY_COLUMN_DATA_TYPE;

    @Override
    protected void switchDatabase(String databaseName) throws SQLException {
        statement.get().execute(String.format(SQL_SWITCH_DATABASE, quote(databaseName)));
    }

    /**
     * Unicode 编码转字符串
     *
     * @param string 支持 Unicode 编码和普通字符混合的字符串
     * @return 解码后的字符串
     */
    public static String unicodeToStr(String string) {
        String prefix = "\\u";
        if (string == null || !string.contains(prefix)) {
            // 传入字符串为空或不包含 Unicode 编码返回原内容
            return string;
        }

        StringBuilder value = new StringBuilder(string.length() >> 2);
        String[] strings = string.split("\\\\u");
        String hex, mix;
        char hexChar;
        int ascii, n;

        if (strings[0].length() > 0) {
            // 处理开头的普通字符串
            value.append(strings[0]);
        }

        try {
            for (int i = 1; i < strings.length; i++) {
                hex = strings[i];
                if (hex.length() > 3) {
                    mix = "";
                    if (hex.length() > 4) {
                        // 处理 Unicode 编码符号后面的普通字符串
                        mix = hex.substring(4);
                    }
                    hex = hex.substring(0, 4);

                    try {
                        Integer.parseInt(hex, 16);
                    } catch (Exception e) {
                        // 不能将当前 16 进制字符串正常转换为 10 进制数字，拼接原内容后跳出
                        value.append(prefix).append(strings[i]);
                        continue;
                    }

                    ascii = 0;
                    for (int j = 0; j < hex.length(); j++) {
                        hexChar = hex.charAt(j);
                        // 将 Unicode 编码中的 16 进制数字逐个转为 10 进制
                        n = Integer.parseInt(String.valueOf(hexChar), 16);
                        // 转换为 ASCII 码
                        ascii += n * ((int) Math.pow(16, (hex.length() - j - 1)));
                    }

                    // 拼接解码内容
                    value.append((char) ascii).append(mix);
                } else {
                    // 不转换特殊长度的 Unicode 编码
                    value.append(prefix).append(hex);
                }
            }
        } catch (Exception e) {
            // Unicode 编码格式有误，解码失败
            return null;
        }

        return value.toString();
    }

    @Override
    protected String quote(String name) {
        return String.format("`%s`", name);
    }

    @Override
    protected List<Object> showTables() throws SQLException {
        List<Object> tables = new ArrayList<>();
        try (ResultSet rs = statement.get().executeQuery(SQL_SHOW_TABLES)) {
           int pos = rs.getMetaData().getColumnCount()==1?1:2;
            while (rs.next()) {
                tables.add(rs.getString(pos));
            }
        }

        return tables;
    }

    @Override
    protected Map<String, Object> queryMetaData(String tableName) throws SQLException {
        Map<String, Object> result = new HashMap<>(16);
        List<Map<String, Object>> columnList = new ArrayList<>();
        List<Map<String, Object>> partitionColumnList = new ArrayList<>();
        Map<String, Object> tableProperties = new HashMap<>(16);

        List<Map<String, String>> metaData = queryData(tableName);
        Iterator<Map<String, String>> it = metaData.iterator();
        int metaDataFlag = 0;
        while(it.hasNext()){
            Map<String, String> lineDataInternal = it.next();
            String colNameInternal = lineDataInternal.get(KEY_COL_NAME);
            if (StringUtils.isBlank(colNameInternal)) {
                continue;
            }
            if(colNameInternal.startsWith("#")){
                colNameInternal = StringUtils.trim(colNameInternal);
                switch (colNameInternal){
                    case PARTITION_INFORMATION:
                        metaDataFlag = 1;
                        break;
                    case TABLE_INFORMATION:
                        metaDataFlag = 2;
                        break;
                    case KEY_RESULTSET_COL_NAME:
                        paraFirst = KEY_RESULTSET_DATA_TYPE;
                        paraSecond = KEY_RESULTSET_COMMENT;
                        break;
                    default:
                        break;
                }
                continue;
            }
            switch (metaDataFlag){
                case 0:
                    columnList.add(parseColumn(lineDataInternal, result.size()));
                    break;
                case 1:
                    partitionColumnList.add(parseColumn(lineDataInternal, result.size()));
                    break;
                case 2:
                    parseTableProperties(lineDataInternal, tableProperties, it);
                    break;
                default:
                    break;
            }
        }

        if (partitionColumnList.size() > 0) {
            List<String> partitionColumnNames = new ArrayList<>();
            for (Map<String, Object> partitionColumn : partitionColumnList) {
                partitionColumnNames.add(partitionColumn.get(KEY_COLUMN_NAME).toString());
            }

            columnList.removeIf(column -> partitionColumnNames.contains(column.get(KEY_COLUMN_NAME).toString()));
            result.put(KEY_PARTITIONS, showPartitions(tableName));
        }
        result.put(KEY_TABLE_PROPERTIES, tableProperties);
        result.put(KEY_PARTITION_COLUMNS, partitionColumnList);
        result.put(KEY_COLUMN, columnList);

        return result;
    }

    private Map<String, Object> parseColumn(Map<String, String> lineDataInternal, int index){
        String dataTypeInternal = lineDataInternal.get(KEY_COLUMN_DATA_TYPE);
        String commentInternal = lineDataInternal.get(KEY_INDEX_COMMENT);
        String colNameInternal = lineDataInternal.get(KEY_COL_NAME);

        Map<String, Object> lineResult = new HashMap<>(16);
        lineResult.put(KEY_COLUMN_NAME, colNameInternal);
        lineResult.put(KEY_COLUMN_TYPE, dataTypeInternal);
        lineResult.put(KEY_COLUMN_COMMENT, unicodeToStr(commentInternal));
        lineResult.put(KEY_COLUMN_INDEX, index);
        return lineResult;
    }

    private String getStoredType(String storedClass) {
        if (storedClass.endsWith(TEXT_FORMAT)){
            return TYPE_TEXT;
        } else if (storedClass.endsWith(ORC_FORMAT)){
            return TYPE_ORC;
        } else if (storedClass.endsWith(PARQUET_FORMAT)){
            return TYPE_PARQUET;
        } else {
            return storedClass;
        }
    }


    private List<Map<String, String>> queryData(String table) throws SQLException{
        try (ResultSet rs = statement.get().executeQuery(String.format(SQL_QUERY_DATA, quote(table)))) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<String> columnNames = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                columnNames.add(metaData.getColumnName(i+1));
            }

            List<Map<String, String>> data = new ArrayList<>();
            while (rs.next()) {
                Map<String, String> lineData = new HashMap<>(Math.max((int) (columnCount/.75f) + 1, 16));
                for (String columnName : columnNames) {
                    lineData.put(columnName, rs.getString(columnName));
                }

                data.add(lineData);
            }

            return data;
        }
    }

    private List<Map<String, String>> showPartitions (String table) throws SQLException{
        List<Map<String, String>> partitions = new ArrayList<>();
        try (ResultSet rs = statement.get().executeQuery(String.format(SQL_SHOW_PARTITIONS, quote(table)))) {
            while (rs.next()) {
                String str = rs.getString(1);
                String[] split = str.split(ConstantValue.EQUAL_SYMBOL);
                if(split.length == 2){
                    Map<String, String> map = new LinkedHashMap<>();
                    map.put(KEY_NAME, split[0]);
                    map.put(KEY_VALUE, split[1]);
                    partitions.add(map);
                }
            }
        }

        return partitions;
    }

    void parseTableProperties(Map<String, String> lineDataInternal, Map<String, Object> tableProperties, Iterator<Map<String, String>> it){
        String name = lineDataInternal.get(KEY_COL_NAME);

        if (name.contains(KEY_COL_LOCATION)) {
            tableProperties.put(KEY_LOCATION, StringUtils.trim(lineDataInternal.get(KEY_COLUMN_DATA_TYPE)));
        }

        if (name.contains(KEY_COL_CREATETIME) || name.contains(KEY_COL_CREATE_TIME)) {
            tableProperties.put(KEY_CREATETIME, StringUtils.trim(lineDataInternal.get(KEY_COLUMN_DATA_TYPE)));
        }

        if (name.contains(KEY_COL_LASTACCESSTIME) || name.contains(KEY_COL_LAST_ACCESS_TIME)) {
            tableProperties.put(KEY_LASTACCESSTIME, StringUtils.trim(lineDataInternal.get(KEY_COLUMN_DATA_TYPE)));
        }

        if (name.contains(KEY_COL_OUTPUTFORMAT)) {
            String storedClass = lineDataInternal.get(KEY_COLUMN_DATA_TYPE);
            tableProperties.put(KEY_STORED_TYPE, getStoredType(storedClass));
        }

        if (name.contains(KEY_COL_TABLE_PARAMETERS)) {
            while (it.hasNext()) {
                lineDataInternal = it.next();
                String nameInternal = lineDataInternal.get(paraFirst);
                if (null == nameInternal) {
                    continue;
                }

                nameInternal = nameInternal.trim();
                if (nameInternal.contains(KEY_INDEX_COMMENT)) {
                    tableProperties.put(KEY_TABLE_COMMENT, StringUtils.trim(unicodeToStr(lineDataInternal.get(paraSecond))));
                }

                if (nameInternal.contains(KEY_TOTALSIZE)) {
                    tableProperties.put(KEY_TOTALSIZE, StringUtils.trim(lineDataInternal.get(paraSecond)));
                }

                if (nameInternal.contains(KEY_TRANSIENT_LASTDDLTIME)) {
                    tableProperties.put(KEY_TRANSIENT_LASTDDLTIME, StringUtils.trim(lineDataInternal.get(paraSecond)));
                }
            }
        }
    }

    @Override
    public Connection getConnection() {
        HiveDbUtil.ConnectionInfo connectionInfo = new HiveDbUtil.ConnectionInfo();
        connectionInfo.setJdbcUrl(dbUrl);
        connectionInfo.setUsername(username);
        connectionInfo.setPassword(password);
        connectionInfo.setHiveConf(hadoopConfig);
        connectionInfo.setDriver(driverName);
        return HiveDbUtil.getConnection(connectionInfo);
    }
}
