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

package com.dtstack.flinkx.metadatahbase.inputformat;

import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;
import com.dtstack.flinkx.metadata.inputformat.MetadataInputSplit;
import com.dtstack.flinkx.metadatahbase.constants.HbaseHelper;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TABLE_PROPERTIES;
import static com.dtstack.flinkx.metadatahbase.constants.HbaseCons.KEY_NAME;
import static com.dtstack.flinkx.metadatahbase.constants.HbaseCons.KEY_REGIONS;

/** 获取元数据
 * @author kunni@dtstack.com
 */
public class MetadatahbaseInputformat extends BaseMetadataInputFormat {

    private static final long serialVersionUID = 1L;

    /**
     * 用于连接hbase的配置
     */
    protected Map<String, Object> hadoopConfig;

    /**
     * hbase 连接
     */
    private transient Connection hbaseConnection;

    /**
     * 因为connection的类型不同，重写该方法
     * @param inputSplit 某个命名空间及需要查询的表
     */
    @Override
    protected void openInternal(InputSplit inputSplit)  throws IOException{
        LOG.info("inputSplit = {}", inputSplit);
        try {
            hbaseConnection = HbaseHelper.getHbaseConnection(hadoopConfig);
            currentDb.set(((MetadataInputSplit) inputSplit).getDbName());
            tableList = ((MetadataInputSplit) inputSplit).getTableList();
            if(CollectionUtils.isEmpty(tableList)){
                tableList = showTables();
            }
            LOG.info("current database = {}, tableSize = {}, tableList = {}",currentDb.get(), tableList.size(), tableList);
            tableIterator.set(tableList.iterator());
        }catch (Exception e){
            throw new IOException(e);
        }
    }

    @Override
    protected List<Object> showTables() throws SQLException {
        List<Object> tableNameList = new LinkedList<>();
        try {
            HTableDescriptor[] tableNames = hbaseConnection.getAdmin().listTableDescriptorsByNamespace(currentDb.get());
            for (HTableDescriptor table : tableNames){
                TableName tableName = table.getTableName();
                // 排除系统表
                if(!tableName.isSystemTable()){
                    tableNameList.add(tableName.getNameAsString());
                }
            }
        }catch (IOException e){
            LOG.error("query table list failed. currentDb = {}, Exception = {}", currentDb.get(), ExceptionUtil.getErrorMessage(e));
            throw new SQLException(e);
        }
        return tableNameList;
    }

    @Override
    protected void switchDatabase(String databaseName) {
        currentDb.set(databaseName);
    }

    @Override
    protected Map<String, Object> queryMetaData(String tableName) throws SQLException {
        Map<String, Object> result = new HashMap<>(16);
        Map<String, Object> tableProperties;
        List<Map<String, Object>> columnList;
        try{
            HTableDescriptor table = hbaseConnection.getAdmin().getTableDescriptor(TableName.valueOf(tableName));
            tableProperties = queryTableProperties(table);
            columnList = queryColumnList(table);
        }catch (IOException e){
            throw new SQLException(e);
        }
        result.put(KEY_TABLE_PROPERTIES, tableProperties);
        result.put(KEY_COLUMN, columnList);
        return result;
    }


    protected Map<String, Object> queryTableProperties(HTableDescriptor table) throws SQLException {
        Map<String, Object> tableProperties = new HashMap<>(16);
        try{
            List<HRegionInfo> regionInfos = hbaseConnection.getAdmin().getTableRegions(table.getTableName());
            tableProperties.put(KEY_REGIONS, regionInfos.size());
        }catch (IOException e){
            LOG.error("query tableProperties failed. {}", ExceptionUtil.getErrorMessage(e));
            throw new SQLException(e);
        }
        return tableProperties;
    }

    protected List<Map<String, Object>> queryColumnList(HTableDescriptor table){
        List<Map<String, Object>> columnList = new ArrayList<>();
        HColumnDescriptor[] columnDescriptors = table.getColumnFamilies();
        for (HColumnDescriptor column : columnDescriptors){
            Map<String, Object> map = new HashMap<>(16);
            map.put(KEY_NAME, column.getNameAsString());
            columnList.add(map);
        }
        return columnList;
    }


    @Override
    protected String quote(String name) {
        return null;
    }

    public void setHadoopConfig(Map<String, Object> hadoopConfig){
        this.hadoopConfig = hadoopConfig;
    }
}
