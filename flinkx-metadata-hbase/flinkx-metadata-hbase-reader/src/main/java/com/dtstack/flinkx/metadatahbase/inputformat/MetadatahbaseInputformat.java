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

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;
import com.dtstack.flinkx.metadata.inputformat.MetadataInputSplit;
import com.dtstack.flinkx.metadatahbase.util.HbaseHelper;
import com.dtstack.flinkx.metadatahbase.util.ZkHelper;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TABLE_CREATE_TIME;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TABLE_PROPERTIES;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TABLE_TOTAL_SIZE;
import static com.dtstack.flinkx.metadatahbase.util.HbaseCons.KEY_NAME;
import static com.dtstack.flinkx.metadatahbase.util.HbaseCons.KEY_REGIONS;
import static com.dtstack.flinkx.metadatahbase.util.ZkHelper.DEFAULT_PATH;

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

    private transient Admin admin;

    private Map<String, Long> createTimeMap;

    /**
     * 因为connection的类型不同，重写该方法
     * @param inputSplit 某个命名空间及需要查询的表
     */
    @Override
    protected void openInternal(InputSplit inputSplit)  throws IOException{
        LOG.info("inputSplit = {}", inputSplit);
        currentDb.set(((MetadataInputSplit) inputSplit).getDbName());
        tableList = ((MetadataInputSplit) inputSplit).getTableList();
        try {
            createTimeMap = queryCreateTimeMap(hadoopConfig);
            hbaseConnection = HbaseHelper.getHbaseConnection(hadoopConfig);
            admin = hbaseConnection.getAdmin();
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
    protected void closeInternal() {
        HbaseHelper.closeAdmin(admin);
        HbaseHelper.closeConnection(hbaseConnection);
    }

    @Override
    protected List<Object> showTables() throws SQLException {
        List<Object> tableNameList = new LinkedList<>();
        try {
            HTableDescriptor[] tableNames = admin.listTableDescriptorsByNamespace(currentDb.get());
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
        tableProperties = queryTableProperties(tableName);
        columnList = queryColumnList(tableName);
        result.put(KEY_TABLE_PROPERTIES, tableProperties);
        result.put(KEY_COLUMN, columnList);
        return result;
    }


    protected Map<String, Object> queryTableProperties(String tableName) throws SQLException {
        Map<String, Object> tableProperties = new HashMap<>(16);
        try{
            HTableDescriptor table = admin.getTableDescriptor(TableName.valueOf(tableName));
            List<HRegionInfo> regionInfos = hbaseConnection.getAdmin().getTableRegions(table.getTableName());
            tableProperties.put(KEY_REGIONS, regionInfos.size());
            // 默认的region大小是256M
            long regionSize = table.getMaxFileSize()==-1 ? 256 : table.getMaxFileSize();
            tableProperties.put(KEY_TABLE_TOTAL_SIZE,  regionSize * regionInfos.size());
            tableProperties.put(KEY_TABLE_CREATE_TIME, createTimeMap.get(table.getNameAsString()));
        }catch (IOException e){
            LOG.error("query tableProperties failed. {}", ExceptionUtil.getErrorMessage(e));
            throw new SQLException(e);
        }
        return tableProperties;
    }

    /**
     * 获取列族信息
     * @return 列族
     */
    protected List<Map<String, Object>> queryColumnList(String tableName) throws SQLException {
        List<Map<String, Object>> columnList = new ArrayList<>();
        try{
            HTableDescriptor table = admin.getTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor[] columnDescriptors = table.getColumnFamilies();
            for (HColumnDescriptor column : columnDescriptors){
                Map<String, Object> map = new HashMap<>(16);
                map.put(KEY_NAME, column.getNameAsString());
                columnList.add(map);
            }
        }catch (IOException e){
            LOG.error("query columnList failed. {}", ExceptionUtil.getErrorMessage(e));
            throw new SQLException(e);
        }
        return columnList;
    }

    protected Map<String, Long> queryCreateTimeMap(Map<String, Object> hadoopConfig) {
        Map<String, Long> createTimeMap = new HashMap<>(16);
        try{
            ZkHelper.createSingleZkClient((String) hadoopConfig.get(HConstants.ZOOKEEPER_QUORUM), ZkHelper.DEFAULT_TIMEOUT);
            List<String> tables = ZkHelper.getChildren(DEFAULT_PATH);
            if(tables != null){
                for(String table : tables){
                    createTimeMap.put(table, ZkHelper.getStat(DEFAULT_PATH + ConstantValue.SINGLE_SLASH_SYMBOL + table));
                }
            }
            ZkHelper.closeZooKeeper();
        }catch (Exception e){
            LOG.error("query createTime map failed, error {}", ExceptionUtil.getErrorMessage(e));
        }
        return createTimeMap;
    }



    @Override
    protected String quote(String name) {
        return name;
    }

    public void setHadoopConfig(Map<String, Object> hadoopConfig){
        this.hadoopConfig = hadoopConfig;
    }
}
