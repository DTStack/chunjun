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
package com.dtstack.flinkx.metadatatidb.reader;

import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.metadatatidb.reader.TidbMetadataCons.*;


/**
 * @author : kunni@dtstack.com
 * @date : 2020/5/26
 */
public class MetadatatidbInputFormat extends BaseMetadataInputFormat {

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
        List<Map<String, String>> partition = queryPartition(tableName);
        Map<String, String> healthy = queryAddPartition(tableName, KEY_HEALTHY);
        Map<String, String> updateTime = queryAddPartition(tableName, KEY_UPDATE_TIME);
        List<Map<String, Object>> partitionColumn = queryPartitionColumn(tableName);
        if(CollectionUtils.size(partition) == 1){
            Map<String, String> perPartition = partition.get(0);
            perPartition.put(KEY_HEALTHY, healthy.get(KEY_HEALTHY));
            perPartition.put(KEY_UPDATE_TIME, updateTime.get(KEY_UPDATE_TIME));
        }else{
            for(Map<String, String> perPartition : partition){
                perPartition.put(KEY_HEALTHY, healthy.get(perPartition.get(KEY_NAME)));
                perPartition.put(KEY_UPDATE_TIME, updateTime.get(KEY_NAME));
            }
        }
        column.removeIf((Map<String, Object> perColumn)->{
            for(Map<String, Object> perPartitionColumn : partitionColumn){
                if(StringUtils.equals((String)perPartitionColumn.get(KEY_NAME), (String)perColumn.get(KEY_NAME))){
                    perPartitionColumn.put(KEY_TYPE, perColumn.get(KEY_TYPE));
                    perPartitionColumn.put(KEY_NULL, perColumn.get(KEY_NULL));
                    perPartitionColumn.put(KEY_DEFAULT, perColumn.get(KEY_DEFAULT));
                    perPartitionColumn.put(KEY_COMMENT, perColumn.get(KEY_COMMENT));
                    perPartitionColumn.put(KEY_INDEX, perColumn.get(KEY_INDEX));
                    return true;
                }
            }
            return false;
        });
        result.put(KEY_TABLEPROPERTIES, tableProp);
        result.put(KEY_COLUMN, column);
        result.put(KEY_PARTITION, partition);
        result.put(KEY_PARTITIONCOLUMN, partitionColumn);
        return result;
    }

    public Map<String, String> queryTableProp(String tableName) throws SQLException {
        Map<String, String> tableProp = new HashMap<>(16);
        String sql = String.format(SQL_QUERY_TABLEINFO, tableName);
        try(Statement st = connection.get().createStatement();
            ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                tableProp.put(KEY_ROWS, rs.getString(KEY_TABLE_ROWS));
                tableProp.put(KEY_TOTALSIZE, rs.getString(KEY_DATA_LENGTH));
                tableProp.put(KEY_CREATETIME, rs.getString(KEY_COLUMN_CREATE_TIME));
                tableProp.put(KEY_COMMENT, rs.getString(KEY_TABLE_COMMENT));
            }
        } catch (SQLException e) {
            throw new SQLException(e.getMessage());
        }
        return tableProp;
    }

    List<Map<String, Object> > queryColumn(String tableName) throws SQLException {
        List<Map<String, Object> > column = new LinkedList<>();
        String sql = String.format(SQL_QUERY_COLUMN, tableName);
        try(Statement st = connection.get().createStatement();
            ResultSet rs = st.executeQuery(sql)) {
            int pos = 1;
            while (rs.next()) {
                Map<String, Object> perColumn = new HashMap<>(16);
                perColumn.put(KEY_NAME, rs.getString(KEY_FIELD));
                perColumn.put(KEY_TYPE, rs.getString(KEY_COLUMN_TYPE));
                perColumn.put(KEY_NULL, rs.getString(KEY_COLUMN_NULL));
                perColumn.put(KEY_DEFAULT, rs.getString(KEY_COLUMN_DEFAULT));
                perColumn.put(KEY_COMMENT, rs.getString(KEY_COLUMN_COMMENT));
                perColumn.put(KEY_INDEX, pos++);
                column.add(perColumn);
            }
        } catch (SQLException e) {
            throw new SQLException(e.getMessage());
        }
        return column;
    }

    List<Map<String, String>> queryPartition(String tableName) throws SQLException {
        List<Map<String, String> > partition = new LinkedList<>();
        String sql = String.format(SQL_QUERY_PARTITION, tableName);
        try(Statement st = connection.get().createStatement();
            ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                Map<String, String> perPartition = new HashMap<>(16);
                perPartition.put(KEY_NAME, rs.getString(KEY_PARTITION_NAME));
                perPartition.put(KEY_CREATETIME, rs.getString(KEY_PARTITION_CREATE_TIME));
                perPartition.put(KEY_ROWS, rs.getString(KEY_PARTITION_TABLE_ROWS));
                perPartition.put(KEY_TOTALSIZE, rs.getString(KEY_PARTITION_DATA_LENGTH));
                partition.add(perPartition);
            }
        } catch (SQLException e) {
            throw new SQLException(e.getMessage());
        }
        return partition;
    }


    Map<String, String> queryAddPartition(String tableName, String msg) throws SQLException {
        Map<String, String> result = new HashMap<>(16);
        String sql = "";
        if(StringUtils.equals(msg, KEY_HEALTHY)){
            sql = String.format(SQL_QUERY_HEALTHY, tableName);
        }else if(StringUtils.equals(msg, KEY_UPDATE_TIME)){
            sql = String.format(SQL_QUERY_UPDATETIME, tableName);
        }
        try(Statement st = connection.get().createStatement();
            ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                /* 考虑partitionName 为空的情况 */
                String name = rs.getString(KEY_PARTITIONNAME);
                if (StringUtils.isNotBlank(name)) {
                    if (StringUtils.equals(msg, KEY_HEALTHY)) {
                        result.put(name, rs.getString(KEY_HEALTHY_HEALTHY));
                    } else if (StringUtils.equals(msg, KEY_UPDATE_TIME)) {
                        result.put(name, rs.getString(KEY_UPDATE_TIME));
                    }
                } else {
                    if (StringUtils.equals(msg, KEY_HEALTHY)) {
                        result.put(KEY_HEALTHY, rs.getString(KEY_HEALTHY_HEALTHY));
                    } else if (StringUtils.equals(msg, KEY_UPDATE_TIME)) {
                        result.put(KEY_UPDATE_TIME, rs.getString(KEY_UPDATE_TIME));
                    }
                }
            }
        } catch (SQLException e) {
            throw new SQLException(e.getMessage());
        }
        return result;
    }

    List<Map<String, Object> > queryPartitionColumn(String tableName) throws SQLException {
        List<Map<String, Object> > partitionColumn = new LinkedList<>();
        String sql = String.format(SQL_QUERY_PARTITIONCOLUMN, tableName);
        try(Statement st = connection.get().createStatement();
            ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                Map<String, Object> perPartitionColumn = new HashMap<>(16);
                String partitionExp = rs.getString(KEY_PARTITION_EXPRESSION);
                if(StringUtils.isNotBlank(partitionExp)){
                    String columnName = partitionExp.substring(partitionExp.indexOf("`")+1, partitionExp.lastIndexOf("`"));
                    perPartitionColumn.put(KEY_NAME, columnName);
                }
                partitionColumn.add(perPartitionColumn);
            }
        } catch (SQLException e) {
            throw new SQLException(e.getMessage());
        }
        return partitionColumn;
    }

}
