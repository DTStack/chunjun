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

import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_FALSE;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_PRIMARY;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TRUE;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_COLUMN;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_COLUMN_COMMENT;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_COLUMN_INDEX;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_COLUMN_NAME;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_COLUMN_TYPE;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_CREATE_TIME;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_DEFAULT;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_HEALTHY;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_NULL;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_PARTITIONS;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_PARTITION_COLUMN;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_PRI;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_ROWS;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_SCALE;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_TABLE_PROPERTIES;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_TES;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_TOTAL_SIZE;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.KEY_UPDATE_TIME;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_COLUMN_DEFAULT;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_COLUMN_NULL;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_COMMENT;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_CREATE_TIME;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_DATA_LENGTH;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_FIELD;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.RESULT_HEALTHY;
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
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.SQL_QUERY_HEALTHY;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.SQL_QUERY_PARTITION;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.SQL_QUERY_PARTITION_COLUMN;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.SQL_QUERY_TABLE_INFO;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.SQL_QUERY_UPDATE_TIME;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.SQL_SHOW_TABLES;
import static com.dtstack.flinkx.metadatatidb.constants.TidbMetadataCons.SQL_SWITCH_DATABASE;


/**
 * @author : kunni@dtstack.com
 * @date : 2020/5/26
 */
public class MetadatatidbInputFormat extends BaseMetadataInputFormat {

    private static final long serialVersionUID = 1L;

    @Override
    protected List<Object> showTables() throws SQLException {
        List<Object> tables = new ArrayList<>();
        try (ResultSet rs = statement.get().executeQuery(SQL_SHOW_TABLES)) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }

        return tables;
    }


    @Override
    protected void switchDatabase(String databaseName) throws SQLException {
        statement.get().execute(String.format(SQL_SWITCH_DATABASE, quote(databaseName)));
    }

    @Override
    protected String quote(String name) {
        return name;
    }

    @Override
    protected Map<String, Object> queryMetaData(String tableName) throws SQLException {
        Map<String, Object> result = new HashMap<>(16);
        Map<String, String> tableProp = queryTableProp(tableName);
        List<Map<String, Object>> column = queryColumn(tableName);
        List<Map<String, Object>> partition = queryPartition(tableName);
        Map<String, Object> healthy = queryAddPartition(tableName, KEY_HEALTHY);
        Map<String, Object> updateTime = queryAddPartition(tableName, KEY_UPDATE_TIME);
        List<Map<String, Object>> partitionColumn = queryPartitionColumn(tableName);
        column.removeIf((Map<String, Object> perColumn)->{
            for(Map<String, Object> perPartitionColumn : partitionColumn){
                if(StringUtils.equals((String)perPartitionColumn.get(KEY_COLUMN_NAME), (String)perColumn.get(KEY_COLUMN_NAME))){
                    perPartitionColumn.put(KEY_COLUMN_TYPE, perColumn.get(KEY_COLUMN_TYPE));
                    perPartitionColumn.put(KEY_NULL, perColumn.get(KEY_NULL));
                    perPartitionColumn.put(KEY_DEFAULT, perColumn.get(KEY_DEFAULT));
                    perPartitionColumn.put(KEY_COLUMN_COMMENT, perColumn.get(KEY_COLUMN_COMMENT));
                    perPartitionColumn.put(KEY_COLUMN_INDEX, perColumn.get(KEY_COLUMN_INDEX));
                    return true;
                }
            }
            return false;
        });
        result.put(KEY_TABLE_PROPERTIES, tableProp);
        result.put(KEY_COLUMN, column);
        if(CollectionUtils.size(partition) > 1){
            for(Map<String, Object> perPartition : partition){
                String columnName = (String)perPartition.get(KEY_COLUMN_NAME);
                perPartition.put(KEY_HEALTHY, healthy.get(columnName));
                perPartition.put(KEY_UPDATE_TIME, updateTime.get(KEY_COLUMN_NAME));
            }
            result.put(KEY_PARTITIONS, partition);
        }
        result.put(KEY_PARTITION_COLUMN, partitionColumn);
        return result;
    }

    public Map<String, String> queryTableProp(String tableName) throws SQLException {
        Map<String, String> tableProp = new HashMap<>(16);
        String sql = String.format(SQL_QUERY_TABLE_INFO, tableName);
        try(Statement st = connection.get().createStatement();
            ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                tableProp.put(KEY_ROWS, rs.getString(RESULT_ROWS));
                tableProp.put(KEY_TOTAL_SIZE, rs.getString(RESULT_DATA_LENGTH));
                tableProp.put(KEY_CREATE_TIME, rs.getString(RESULT_CREATE_TIME));
                tableProp.put(KEY_COLUMN_COMMENT, rs.getString(RESULT_COMMENT));
            }
        } catch (SQLException e) {
            throw new SQLException(e.getMessage());
        }
        return tableProp;
    }

    protected List<Map<String, Object> > queryColumn(String tableName) throws SQLException {
        List<Map<String, Object> > column = new LinkedList<>();
        String sql = String.format(SQL_QUERY_COLUMN, tableName);
        try(Statement st = connection.get().createStatement();
            ResultSet rs = st.executeQuery(sql)) {
            int pos = 1;
            while (rs.next()) {
                Map<String, Object> perColumn = new HashMap<>(16);
                perColumn.put(KEY_COLUMN_NAME, rs.getString(RESULT_FIELD));
                String type = rs.getString(RESULT_TYPE);
                perColumn.put(KEY_COLUMN_TYPE, type);
                perColumn.put(KEY_NULL, StringUtils.equals(rs.getString(RESULT_COLUMN_NULL), KEY_TES) ? KEY_TRUE : KEY_FALSE);
                perColumn.put(KEY_PRIMARY, StringUtils.equals(rs.getString(RESULT_KEY), KEY_PRI) ? KEY_TRUE : KEY_FALSE);
                perColumn.put(KEY_DEFAULT, rs.getString(RESULT_COLUMN_DEFAULT));
                perColumn.put(KEY_SCALE, StringUtils.contains(type, '(') ? StringUtils.substring(type, type.indexOf("(")+1, type.indexOf(")")) : 0);
                perColumn.put(KEY_COLUMN_COMMENT, rs.getString(RESULT_COMMENT));
                perColumn.put(KEY_COLUMN_INDEX, pos++);
                column.add(perColumn);
            }
        } catch (SQLException e) {
            throw new SQLException(e.getMessage());
        }
        return column;
    }

    protected List<Map<String, Object>> queryPartition(String tableName) throws SQLException {
        List<Map<String, Object> > partition = new LinkedList<>();
        String sql = String.format(SQL_QUERY_PARTITION, tableName);
        try(Statement st = connection.get().createStatement();
            ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                Map<String, Object> perPartition = new HashMap<>(16);
                perPartition.put(KEY_COLUMN_NAME, rs.getString(RESULT_PARTITION_NAME));
                perPartition.put(KEY_CREATE_TIME, rs.getString(RESULT_PARTITION_CREATE_TIME));
                perPartition.put(KEY_ROWS, rs.getInt(RESULT_PARTITION_TABLE_ROWS));
                perPartition.put(KEY_TOTAL_SIZE, rs.getLong(RESULT_PARTITION_DATA_LENGTH));
                partition.add(perPartition);
            }
        } catch (SQLException e) {
            throw new SQLException(e.getMessage());
        }
        return partition;
    }


    protected Map<String, Object> queryAddPartition(String tableName, String msg) throws SQLException {
        Map<String, Object> result = new HashMap<>(16);
        String sql = "";
        if(StringUtils.equals(msg, KEY_HEALTHY)){
            sql = String.format(SQL_QUERY_HEALTHY, tableName);
        }else if(StringUtils.equals(msg, KEY_UPDATE_TIME)){
            sql = String.format(SQL_QUERY_UPDATE_TIME, tableName);
        }
        try(Statement st = connection.get().createStatement();
            ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                /* 考虑partitionName 为空的情况 */
                String name = rs.getString(RESULT_PARTITIONNAME);
                if (StringUtils.isNotBlank(name)) {
                    if (StringUtils.equals(msg, KEY_HEALTHY)) {
                        result.put(name, rs.getInt(RESULT_HEALTHY));
                    } else if (StringUtils.equals(msg, KEY_UPDATE_TIME)) {
                        result.put(name, rs.getString(RESULT_UPDATE_TIME));
                    }
                } else {
                    if (StringUtils.equals(msg, KEY_HEALTHY)) {
                        result.put(KEY_HEALTHY, rs.getInt(RESULT_HEALTHY));
                    } else if (StringUtils.equals(msg, KEY_UPDATE_TIME)) {
                        result.put(KEY_UPDATE_TIME, rs.getString(RESULT_UPDATE_TIME));
                    }
                }
            }
        } catch (SQLException e) {
            throw new SQLException(e.getMessage());
        }
        return result;
    }

    protected List<Map<String, Object> > queryPartitionColumn(String tableName) throws SQLException {
        List<Map<String, Object> > partitionColumn = new LinkedList<>();
        String sql = String.format(SQL_QUERY_PARTITION_COLUMN, tableName);
        try(Statement st = connection.get().createStatement();
            ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                Map<String, Object> perPartitionColumn = new HashMap<>(16);
                String partitionExp = rs.getString(RESULT_PARTITION_EXPRESSION);
                if(StringUtils.isNotBlank(partitionExp)){
                    String columnName = partitionExp.substring(partitionExp.indexOf("`")+1, partitionExp.lastIndexOf("`"));
                    perPartitionColumn.put(KEY_COLUMN_NAME, columnName);
                }
                partitionColumn.add(perPartitionColumn);
            }
        } catch (SQLException e) {
            throw new SQLException(e.getMessage());
        }
        return partitionColumn;
    }

}
