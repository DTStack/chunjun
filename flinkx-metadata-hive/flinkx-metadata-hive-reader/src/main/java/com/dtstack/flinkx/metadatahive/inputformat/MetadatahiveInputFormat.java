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
package com.dtstack.flinkx.metadatahive.inputformat;

import com.dtstack.flinkx.metadatahive.constants.HiveDbUtil;
import com.dtstack.flinkx.metadatahive.entity.HiveConnectionInfo;
import com.dtstack.flinkx.metatdata.hive.core.entity.HiveTableEntity;
import com.dtstack.flinkx.metatdata.hive.core.entity.MetadataHiveEntity;
import com.dtstack.flinkx.metatdata.hive.core.util.HiveOperatorUtils;
import com.dtstack.metadata.rdb.core.entity.ColumnEntity;
import com.dtstack.metadata.rdb.core.entity.MetadatardbEntity;
import com.dtstack.metadata.rdb.inputformat.MetadatardbInputFormat;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.KEY_COLUMN_DATA_TYPE;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.KEY_COL_CREATETIME;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.KEY_COL_CREATE_TIME;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.KEY_COL_LASTACCESSTIME;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.KEY_COL_LAST_ACCESS_TIME;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.KEY_COL_LOCATION;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.KEY_COL_NAME;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.KEY_COL_OUTPUTFORMAT;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.KEY_COL_TABLE_PARAMETERS;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.KEY_COMMENT;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.KEY_RESULTSET_COL_NAME;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.KEY_RESULTSET_DATA_TYPE;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.KEY_TOTALSIZE;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.KEY_TRANSIENT_LASTDDLTIME;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.PARTITION_INFORMATION;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.SQL_QUERY_DATA;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.SQL_SHOW_TABLES;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.SQL_SWITCH_DATABASE;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.TABLE_INFORMATION;

/**
 * @author : tiezhu
 * @date : 2020/3/9
 */
public class MetadatahiveInputFormat extends MetadatardbInputFormat {

    protected static final long serialVersionUID = 1L;

    /**hive 配置信息*/
    protected Map<String, Object> hadoopConfig;

    String paraFirst = KEY_COL_NAME;
    String paraSecond = KEY_COLUMN_DATA_TYPE;

    @Override
    public List<Object> showTables() throws SQLException {
        List<Object> tables = new ArrayList<>();
        try (ResultSet rs = statement.executeQuery(SQL_SHOW_TABLES)) {
            int pos = rs.getMetaData().getColumnCount() == 1 ? 1 : 2;
            while (rs.next()) {
                tables.add(rs.getString(pos));
            }
        }

        return tables;
    }


    @Override
    public void switchDataBase() throws SQLException {
        statement.execute(String.format(SQL_SWITCH_DATABASE, quote(currentDatabase)));
    }

    @Override
    public MetadatardbEntity createMetadatardbEntity() throws IOException {
        MetadataHiveEntity metadataHive2Entity = new MetadataHiveEntity();
        List<ColumnEntity> columnList = new ArrayList<>();
        List<ColumnEntity> partitionColumnList = new ArrayList<>();
        HiveTableEntity tableProperties = new HiveTableEntity();
        String tableName = (String) currentObject;
        List<Map<String, String>> metaData;
        try {
            metaData = queryData(tableName);
        } catch (SQLException e) {
            throw new IOException("read metadata failed " + e.getMessage(), e);
        }
        Iterator<Map<String, String>> it = metaData.iterator();
        int metaDataFlag = 0;
        while (it.hasNext()) {
            Map<String, String> lineDataInternal = it.next();
            String colNameInternal = lineDataInternal.get(KEY_COL_NAME);
            if (StringUtils.isBlank(colNameInternal)) {
                continue;
            }
            if (colNameInternal.startsWith("#")) {
                colNameInternal = StringUtils.trim(colNameInternal);
                switch (colNameInternal) {
                    case PARTITION_INFORMATION:
                        metaDataFlag = 1;
                        break;
                    case TABLE_INFORMATION:
                        metaDataFlag = 2;
                        break;
                    case KEY_RESULTSET_COL_NAME:
                        paraFirst = KEY_RESULTSET_DATA_TYPE;
                        paraSecond = KEY_COMMENT;
                        break;
                    default:
                        break;
                }
                continue;
            }
            switch (metaDataFlag) {
                case 0:
                    columnList.add(parseColumn(lineDataInternal, columnList.size() + 1));
                    break;
                case 1:
                    partitionColumnList.add(parseColumn(lineDataInternal, partitionColumnList.size() + 1));
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
            for (ColumnEntity partitionColumn : partitionColumnList) {
                partitionColumnNames.add(partitionColumn.getName());
            }
            columnList.removeIf(column -> partitionColumnNames.contains(column.getName()));
            metadataHive2Entity.setPartitions(partitionColumnNames);
        }
        metadataHive2Entity.setTableProperties(tableProperties);
        metadataHive2Entity.setPartitionColumns(partitionColumnList);
        metadataHive2Entity.setColumns(columnList);
        return metadataHive2Entity;
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

    protected String quote(String name) {
        return String.format("`%s`", name);
    }


    /**
     * 解析字段信息
     *
     * @param lineDataInternal
     * @param index
     * @return
     */
    private ColumnEntity parseColumn(Map<String, String> lineDataInternal, int index) {
        ColumnEntity hiveColumnEntity = new ColumnEntity();
        String dataTypeInternal = lineDataInternal.get(KEY_COLUMN_DATA_TYPE);
        String commentInternal = lineDataInternal.get(KEY_COMMENT);
        String colNameInternal = lineDataInternal.get(KEY_COL_NAME);
        hiveColumnEntity.setType(dataTypeInternal);
        hiveColumnEntity.setComment(commentInternal);
        hiveColumnEntity.setName(colNameInternal);
        hiveColumnEntity.setIndex(index);
        return hiveColumnEntity;
    }

    /**
     * 查询表元数据
     *
     * @param table
     * @return
     * @throws SQLException
     */
    private List<Map<String, String>> queryData(String table) throws SQLException {
        try (ResultSet rs = statement.executeQuery(String.format(SQL_QUERY_DATA, quote(table)))) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<String> columnNames = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                columnNames.add(metaData.getColumnName(i + 1));
            }
            List<Map<String, String>> data = new ArrayList<>();
            while (rs.next()) {
                Map<String, String> lineData = new HashMap<>(Math.max((int) (columnCount / .75f) + 1, 16));
                for (String columnName : columnNames) {
                    lineData.put(columnName, rs.getString(columnName));
                }
                data.add(lineData);
            }
            return data;
        }
    }

    /**
     * 解析表的参数
     *
     * @param lineDataInternal
     * @param tableProperties
     * @param it
     */
    void parseTableProperties(Map<String, String> lineDataInternal, HiveTableEntity tableProperties, Iterator<Map<String, String>> it) {
        String name = lineDataInternal.get(KEY_COL_NAME);

        if (name.contains(KEY_COL_LOCATION)) {
            tableProperties.setLocation(StringUtils.trim(lineDataInternal.get(KEY_COLUMN_DATA_TYPE)));
        }
        if (name.contains(KEY_COL_CREATETIME) || name.contains(KEY_COL_CREATE_TIME)) {
            tableProperties.setCreateTime(StringUtils.trim(lineDataInternal.get(KEY_COLUMN_DATA_TYPE)));
        }
        if (name.contains(KEY_COL_LASTACCESSTIME) || name.contains(KEY_COL_LAST_ACCESS_TIME)) {
            tableProperties.setLastAccessTime(StringUtils.trim(lineDataInternal.get(KEY_COLUMN_DATA_TYPE)));
        }
        if (name.contains(KEY_COL_OUTPUTFORMAT)) {
            String storedClass = lineDataInternal.get(KEY_COLUMN_DATA_TYPE);
            tableProperties.setStoreType(HiveOperatorUtils.getStoredType(storedClass));
        }
        if (name.contains(KEY_COL_TABLE_PARAMETERS)) {
            while (it.hasNext()) {
                lineDataInternal = it.next();
                String nameInternal = lineDataInternal.get(paraFirst);
                if (null == nameInternal) {
                    continue;
                }

                nameInternal = nameInternal.trim();
                if (nameInternal.contains(KEY_COMMENT)) {
                    tableProperties.setComment(StringUtils.trim(unicodeToStr(lineDataInternal.get(paraSecond))));
                }

                if (nameInternal.contains(KEY_TOTALSIZE)) {
                    tableProperties.setTotalSize(MapUtils.getLong(lineDataInternal, paraSecond));
                }

                if (nameInternal.contains(KEY_TRANSIENT_LASTDDLTIME)) {
                    tableProperties.setTransientLastDdlTime(MapUtils.getString(lineDataInternal, paraSecond));
                }
            }
        }
    }

    @Override
    public Connection getConnection() {
        HiveConnectionInfo hiveConnectionInfo = new HiveConnectionInfo(connectionInfo);
        hiveConnectionInfo.setHiveConf(hadoopConfig);
        return HiveDbUtil.getConnection(hiveConnectionInfo);
    }
}
