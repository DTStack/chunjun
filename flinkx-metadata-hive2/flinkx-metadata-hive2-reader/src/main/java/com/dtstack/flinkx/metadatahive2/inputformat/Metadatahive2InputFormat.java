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
package com.dtstack.flinkx.metadatahive2.inputformat;

import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/9
 */
public class Metadatahive2InputFormat extends BaseMetadataInputFormat {

    private static final String TEXT_FORMAT = "TextOutputFormat";
    private static final String ORC_FORMAT = "OrcOutputFormat";
    private static final String PARQUET_FORMAT = "MapredParquetOutputFormat";

    @Override
    protected List<String> showDatabases(Connection connection) throws SQLException {
        List<String> dbNameList = new ArrayList<>();
        try(Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("show databases")) {
            while (rs.next()) {
                dbNameList.add(rs.getString(1));
            }
        }

        return dbNameList;
    }

    @Override
    protected void switchDatabase(String database) throws SQLException {
        statement.get().execute(String.format("use %s", quote(database)));
    }

    @Override
    protected List<String> showTables() throws SQLException {
        List<String> tables = new ArrayList<>();
        try (ResultSet rs = statement.get().executeQuery("show tables")) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }

        return tables;
    }

    @Override
    protected String quote(String value) {
        return String.format("`%s`", value);
    }

    @Override
    protected Map<String, Object> queryMetaData(String table) throws SQLException {
        Map<String, Object> result = new HashMap<>(16);

        List<Map<String, String>> metaData = queryData(table);

        Map<String, Object> tableProperties = parseTableProperties(metaData);
        result.put("tableProperties", tableProperties);

        List<Map<String, Object>> partitionColumnList = parsePartitionColumn(metaData);
        result.put("partitionColumn", partitionColumnList);

        List<Map<String, Object>> columnList = parseColumn(metaData);
        if (partitionColumnList.size() > 0) {
            List<String> partitionColumnNames = new ArrayList<>();
            for (Map<String, Object> partitionColumn : partitionColumnList) {
                partitionColumnNames.add(partitionColumn.get("name").toString());
            }

            columnList.removeIf(column -> partitionColumnNames.contains(column.get("name").toString()));
        }

        result.put("column", columnList);

        if (partitionColumnList.size() > 0) {
            result.put("partitions", showPartitions(table));
        }

        return result;
    }

    /**
     * "comment": "this is tableA",
     *                 "totalSize": 12,
     */
    private Map<String, Object> parseTableProperties(List<Map<String, String>> metaData) {
        Map<String, Object> tableProperties = new HashMap<>(16);

        Iterator<Map<String, String>> it = metaData.iterator();
        while (it.hasNext()) {
            Map<String, String> metaDatum = it.next();
            String name = metaDatum.get("col_name");
            if (null == name) {
                continue;
            }

            name = name.trim();
            if (name.length() == 0 || name.startsWith("#")) {
                continue;
            }

            if (name.contains("Location:")) {
                tableProperties.put("location", getData(metaDatum.get("data_type")));
            }

            if (name.contains("CreateTime:")) {
                tableProperties.put("createTime", getData(metaDatum.get("data_type")));
            }

            if (name.contains("LastAccessTime:")) {
                tableProperties.put("lastAccessTime", getData(metaDatum.get("data_type")));
            }

            if (name.contains("OutputFormat:")) {
                String storedClass = metaDatum.get("data_type");
                tableProperties.put("storedType", getStoredType(storedClass));
            }

            if (name.contains("Table Parameters:")) {
                while (it.hasNext()) {
                    metaDatum = it.next();
                    String nameInternal = metaDatum.get("data_type");
                    if (null == nameInternal) {
                        continue;
                    }

                    nameInternal = nameInternal.trim();
                    if (nameInternal.contains("comment")) {
                        tableProperties.put("comment", getData(metaDatum.get("comment")));
                    }

                    if (nameInternal.contains("totalSize")) {
                        tableProperties.put("totalSize", getData(metaDatum.get("comment")));
                    }

                    if (nameInternal.contains("transient_lastDdlTime")) {
                        tableProperties.put("transient_lastDdlTime", getData(metaDatum.get("comment")));
                    }
                }
            }
        }

        return tableProperties;
    }

    private String getData(String data) {
        if (null == data) {
            return null;
        }

        return data.trim();
    }

    private String getStoredType(String storedClass) {
        if (storedClass.endsWith(TEXT_FORMAT)){
            return "text";
        } else if (storedClass.endsWith(ORC_FORMAT)){
            return "orc";
        } else if (storedClass.endsWith(PARQUET_FORMAT)){
            return "parquet";
        } else {
            return storedClass;
        }
    }

    private List<Map<String, Object>> parseColumn(List<Map<String, String>> metaData) {
        List<Map<String, Object>> result = new ArrayList<>();

        Iterator<Map<String, String>> it = metaData.iterator();
        while (it.hasNext()) {
            Map<String, String> lineData = it.next();
            String colName = lineData.get("col_name");
            if (null == colName) {
                continue;
            }

            colName = colName.trim();

            if (StringUtils.isNotEmpty(colName) && "# col_name".equals(colName)) {
                while (it.hasNext()) {
                    Map<String, String> lineDataInternal = it.next();
                    String colNameInternal = lineDataInternal.get("col_name");
                    if (null == colNameInternal) {
                        continue;
                    }

                    colNameInternal = colNameInternal.trim();
                    if (colNameInternal.trim().length() == 0) {
                        continue;
                    }

                    if (colNameInternal.startsWith("#")) {
                        break;
                    }

                    String dataTypeInternal = lineDataInternal.get("data_type");
                    String commentInternal = lineDataInternal.get("comment");

                    Map<String, Object> lineResult = new HashMap<>(4);
                    lineResult.put("name", colNameInternal);
                    lineResult.put("type", dataTypeInternal);
                    lineResult.put("comment", commentInternal);
                    lineResult.put("index", result.size());

                    result.add(lineResult);
                }
            }
        }

        return result;
    }

    private List<Map<String, Object>> parsePartitionColumn(List<Map<String, String>> metaData) {
        List<Map<String, Object>> result = new ArrayList<>();

        Iterator<Map<String, String>> it = metaData.iterator();
        while (it.hasNext()) {
            Map<String, String> lineData = it.next();
            String colName = lineData.get("col_name");
            if (null == colName) {
                continue;
            }

            colName = colName.trim();

            if (StringUtils.isNotEmpty(colName) && "# Partition Information".equals(colName)) {
                it.next();
                while (it.hasNext()) {
                    Map<String, String> lineDataInternal = it.next();
                    String colNameInternal = lineDataInternal.get("col_name");
                    if (null == colNameInternal) {
                        continue;
                    }

                    colNameInternal = colNameInternal.trim();
                    if (colNameInternal.trim().length() == 0) {
                        continue;
                    }

                    if (colNameInternal.startsWith("#")) {
                        break;
                    }

                    String dataTypeInternal = lineDataInternal.get("data_type");
                    String commentInternal = lineDataInternal.get("comment");

                    Map<String, Object> lineResult = new HashMap<>(4);
                    lineResult.put("name", colNameInternal);
                    lineResult.put("type", dataTypeInternal);
                    lineResult.put("comment", commentInternal);
                    lineResult.put("index", result.size());

                    result.add(lineResult);
                }
            }
        }

        return result;
    }

    private List<Map<String, String>> queryData(String table) throws SQLException{
        try (ResultSet rs = statement.get().executeQuery(String.format("desc formatted %s", quote(table)))) {
            ResultSetMetaData metaData = rs.getMetaData();

            List<String> columnNames = new ArrayList<>();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                columnNames.add(metaData.getColumnName(i+1));
            }

            List<Map<String, String>> data = new ArrayList<>();
            while (rs.next()) {
                Map<String, String> lineData = new HashMap<>(16);
                for (String columnName : columnNames) {
                    lineData.put(columnName, rs.getString(columnName));
                }

                data.add(lineData);
            }

            return data;
        }
    }

    private List<String> showPartitions (String table) throws SQLException{
        List<String> partitions = new ArrayList<>();
        try (ResultSet rs = statement.get().executeQuery(String.format("show partitions %s", quote(table)))) {
            while (rs.next()) {
                partitions.add(rs.getString(1));
            }
        }

        return partitions;
    }
}
