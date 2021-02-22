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
package com.dtstack.flinkx.metadatatidb.inputformat;

import com.dtstack.flinkx.metadatamysql.entity.MysqlColumnEntity;
import com.dtstack.flinkx.metadatamysql.entity.MysqlTableEntity;
import com.dtstack.flinkx.metadatamysql.inputformat.MetadatamysqlInputFormat;
import com.dtstack.flinkx.metadatatidb.entity.MetadataTidbEntity;
import com.dtstack.flinkx.metadatatidb.entity.TidbPartitionEntity;
import com.dtstack.metadata.rdb.core.entity.MetadatardbEntity;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_PRI;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_TES;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_UPDATE_TIME;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_COLUMN_DEFAULT;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_COLUMN_NULL;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_COMMENT;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_CREATE_TIME;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_DATA_LENGTH;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_FIELD;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_KEY;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_PARTITIONNAME;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_PARTITION_CREATE_TIME;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_PARTITION_DATA_LENGTH;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_PARTITION_EXPRESSION;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_PARTITION_NAME;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_PARTITION_TABLE_ROWS;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_ROWS;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_TYPE;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_UPDATE_TIME;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.SQL_QUERY_COLUMN;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.SQL_QUERY_PARTITION;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.SQL_QUERY_PARTITION_COLUMN;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.SQL_QUERY_TABLE_INFO;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.SQL_QUERY_UPDATE_TIME;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.SQL_SHOW_TABLES;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.KEY_FALSE;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.KEY_TRUE;

/**
 * @author : kunni@dtstack.com
 * @date : 2020/5/26
 */
public class MetadatatidbInputFormat extends MetadatamysqlInputFormat {

    private static final long serialVersionUID = 1L;

    @Override
    public List<Object> showTables() throws SQLException {
        List<Object> tables = new ArrayList<>();
        try (ResultSet rs = statement.executeQuery(SQL_SHOW_TABLES)) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        return tables;
    }

    @Override
    public MetadatardbEntity createMetadatardbEntity() throws IOException {
        try {
            return queryMetaData();
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e);
        }
    }


    protected MetadataTidbEntity queryMetaData() throws SQLException {
        MetadataTidbEntity metadataTidbEntity = new MetadataTidbEntity();
        MysqlTableEntity tableProp = queryTableProp();
        List<MysqlColumnEntity> columns = queryColumn(null);
        List<TidbPartitionEntity> partitions = queryPartition();
        Map<String, String> updateTime = queryAddPartition();
        List<String> partitionColumns = queryPartitionColumn();
        List<MysqlColumnEntity> partitionsColumnEntities = new ArrayList<>();
        columns.removeIf((MysqlColumnEntity perColumn) -> {
            for (String partitionColumn : partitionColumns) {
                if (StringUtils.equals(partitionColumn, perColumn.getName())) {
                    //copy属性值
                    partitionsColumnEntities.add(perColumn);
                    return true;
                }
            }
            return false;
        });
        metadataTidbEntity.setTableProperties(tableProp);
        metadataTidbEntity.setColumns(columns);
        if (CollectionUtils.size(partitions) > 1) {
            for (TidbPartitionEntity tidbPartitionEntity : partitions) {
                String columnName = tidbPartitionEntity.getColumnName();
                tidbPartitionEntity.setCreateTime(MapUtils.getString(updateTime, columnName) == null ? updateTime.get(KEY_UPDATE_TIME) : MapUtils.getString(updateTime, columnName));
            }
            metadataTidbEntity.setPartitions(partitions);
        }
        metadataTidbEntity.setPartitionColumnEntities(partitionsColumnEntities);
        return metadataTidbEntity;
    }

    public MysqlTableEntity queryTableProp() throws SQLException {
        String tableName = (String) currentObject;
        MysqlTableEntity mysqlTableEntity = new MysqlTableEntity();
        String sql = String.format(SQL_QUERY_TABLE_INFO, tableName);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                mysqlTableEntity.setRows(Long.valueOf(rs.getString(RESULT_ROWS)));
                mysqlTableEntity.setTotalSize(Long.valueOf(rs.getString(RESULT_DATA_LENGTH)));
                mysqlTableEntity.setCreateTime(rs.getString(RESULT_CREATE_TIME));
                mysqlTableEntity.setComment(rs.getString(RESULT_COMMENT));
            }
        }
        return mysqlTableEntity;
    }

    @Override
    public List<MysqlColumnEntity> queryColumn(String schema) throws SQLException {
        String tableName = (String) currentObject;
        List<MysqlColumnEntity> columns = new LinkedList<>();
        String sql = String.format(SQL_QUERY_COLUMN, tableName);
        try (ResultSet rs = statement.executeQuery(sql)) {
            int pos = 1;
            while (rs.next()) {
                MysqlColumnEntity columnEntity = new MysqlColumnEntity();
                columnEntity.setName(rs.getString(RESULT_FIELD));
                String type = rs.getString(RESULT_TYPE);
                columnEntity.setType(type);
                columnEntity.setNullAble(StringUtils.equals(rs.getString(RESULT_COLUMN_NULL), KEY_TES) ? KEY_TRUE : KEY_FALSE);
                columnEntity.setPrimaryKey(StringUtils.equals(rs.getString(RESULT_KEY), KEY_PRI) ? KEY_TRUE : KEY_FALSE);
                columnEntity.setDefaultValue(rs.getString(RESULT_COLUMN_DEFAULT));
                String length = StringUtils.contains(type, '(') ? StringUtils.substring(type, type.indexOf("(") + 1, type.indexOf(")")) : "0";
                columnEntity.setLength(Integer.valueOf(length));
                columnEntity.setComment(rs.getString(RESULT_COMMENT));
                columnEntity.setIndex(pos++);
                columns.add(columnEntity);
            }
        }
        return columns;
    }

    protected List<TidbPartitionEntity> queryPartition() throws SQLException {
        String tableName = (String) currentObject;
        List<TidbPartitionEntity> partitions = new LinkedList<>();
        String sql = String.format(SQL_QUERY_PARTITION, tableName);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                TidbPartitionEntity tidbPartitionEntity = new TidbPartitionEntity();
                tidbPartitionEntity.setColumnName(rs.getString(RESULT_PARTITION_NAME));
                tidbPartitionEntity.setCreateTime(rs.getString(RESULT_PARTITION_CREATE_TIME));
                tidbPartitionEntity.setPartitionRows(rs.getLong(RESULT_PARTITION_TABLE_ROWS));
                tidbPartitionEntity.setPartitionSize(rs.getLong(RESULT_PARTITION_DATA_LENGTH));
                partitions.add(tidbPartitionEntity);
            }
        }
        return partitions;
    }


    protected Map<String, String> queryAddPartition() throws SQLException {
        String tableName = (String) currentObject;
        Map<String, String> result = new HashMap<>(16);
        String sql = String.format(SQL_QUERY_UPDATE_TIME, tableName);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                /* 考虑partitionName 为空的情况 */
                String name = rs.getString(RESULT_PARTITIONNAME);
                if (StringUtils.isNotBlank(name)) {
                    result.put(name, rs.getString(RESULT_UPDATE_TIME));
                } else {
                    result.put(KEY_UPDATE_TIME, rs.getString(RESULT_UPDATE_TIME));
                }
            }
        }
        return result;
    }

    protected List<String> queryPartitionColumn() throws SQLException {
        String tableName = (String) currentObject;
        List<String> partitionColumns = new LinkedList<>();
        String sql = String.format(SQL_QUERY_PARTITION_COLUMN, tableName);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                String partitionExp = rs.getString(RESULT_PARTITION_EXPRESSION);
                if (StringUtils.isNotBlank(partitionExp)) {
                    String columnName = partitionExp.substring(partitionExp.indexOf("`") + 1, partitionExp.lastIndexOf("`"));
                    partitionColumns.add(columnName);
                }
            }
        }
        return partitionColumns;
    }
}
