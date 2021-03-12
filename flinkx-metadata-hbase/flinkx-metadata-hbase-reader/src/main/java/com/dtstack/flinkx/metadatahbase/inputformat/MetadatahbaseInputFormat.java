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
import com.dtstack.flinkx.enums.SizeUnitType;
import com.dtstack.flinkx.metadata.core.entity.MetadataEntity;
import com.dtstack.flinkx.metadata.inputformat.MetadataBaseInputFormat;
import com.dtstack.flinkx.metadatahbase.entity.HbaseColumnEntity;
import com.dtstack.flinkx.metadatahbase.entity.HbaseTableEntity;
import com.dtstack.flinkx.metadatahbase.entity.MetadataHbaseEntity;
import com.dtstack.flinkx.metadatahbase.util.HbaseHelper;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.ZkHelper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.constants.ConstantValue.COMMA_SYMBOL;
import static com.dtstack.flinkx.util.ZkHelper.APPEND_PATH;

/**
 * 获取元数据
 *
 * @author kunni@dtstack.com
 */
public class MetadatahbaseInputFormat extends MetadataBaseInputFormat {

    private static final long serialVersionUID = 1L;

    /**用于连接hbase的配置*/
    protected Map<String, Object> hadoopConfig;

    /**hbase 连接*/
    protected Connection hbaseConnection;

    /**hbase admin*/
    protected Admin admin;

    /**hbase 建表时间的集合*/
    protected Map<String, Long> createTimeMap;

    /**hbase 表大小的集合*/
    protected Map<String, Integer> tableSizeMap;

    /**hbase zookeeper信息*/
    protected ZooKeeper zooKeeper;

    /**hbase znode路径*/
    protected String path;

    @Override
    protected void doOpenInternal() throws IOException {
        try {
            if (hbaseConnection == null) {
                hbaseConnection = HbaseHelper.getHbaseConnection(hadoopConfig);
                admin = hbaseConnection.getAdmin();
            }
            createTimeMap = queryCreateTimeMap(hadoopConfig);
            tableSizeMap = generateTableSizeMap();
            if (CollectionUtils.isEmpty(tableList)) {
                tableList = showTables();
            }
            LOG.info("current database = {}, tableSize = {}, tableList = {}", currentDatabase, tableList.size(), tableList);
            iterator = tableList.iterator();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public MetadataEntity createMetadataEntity() throws Exception {
        MetadataHbaseEntity entity = new MetadataHbaseEntity();
        entity.setTableName((String) currentObject);
        entity.setSchema(currentDatabase);
        String tableName = String.format("%s:%s", currentDatabase, currentObject);
        entity.setColumns(queryColumnList(tableName));
        entity.setTableProperties(queryTableProperties(tableName));
        return entity;
    }

    /**
     * 获取表的region大小的总和即为表饿的存储大小，误差最大为1M * regionSize
     *
     * @return
     * @throws Exception
     */
    private Map<String, Integer> generateTableSizeMap() throws Exception {
        Map<String, Integer> sizeMap = new HashMap<>(16);
        ClusterStatus clusterStatus = admin.getClusterStatus();
        for (ServerName serverName : clusterStatus.getServers()) {
            ServerLoad serverLoad = clusterStatus.getLoad(serverName);
            for (Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
                RegionLoad regionLoad = entry.getValue();
                String regionName = new String(entry.getKey(), "UTF-8");
                String[] regionSplits = regionName.split(COMMA_SYMBOL);
                //regionSplits[0] 为table name
                int sumSize = sizeMap.getOrDefault(regionSplits[0], 0) + regionLoad.getStorefileSizeMB();
                sizeMap.put(regionSplits[0], sumSize);
            }
        }
        return sizeMap;
    }

    @Override
    protected void closeInternal() {
    }

    protected List<Object> showTables() throws SQLException {
        List<Object> tableNameList = new LinkedList<>();
        try {
            HTableDescriptor[] tableNames = admin.listTableDescriptorsByNamespace(currentDatabase);
            for (HTableDescriptor table : tableNames) {
                TableName tableName = table.getTableName();
                // 排除系统表
                if (!tableName.isSystemTable()) {
                    //此时的表名带有namespace,需要去除
                    String tableWithNameSpace = tableName.getNameAsString();
                    if (tableWithNameSpace.contains(ConstantValue.COLON_SYMBOL)) {
                        tableWithNameSpace = tableWithNameSpace.split(ConstantValue.COLON_SYMBOL)[1];
                    }
                    tableNameList.add(tableWithNameSpace);
                }
            }
        } catch (IOException e) {
            LOG.error("query table list failed. currentDb = {}, Exception = {}", currentDatabase, ExceptionUtil.getErrorMessage(e));
            throw new SQLException(e);
        }
        return tableNameList;
    }

    @Override
    public void closeInputFormat() throws IOException {
        HbaseHelper.closeAdmin(admin);
        HbaseHelper.closeConnection(hbaseConnection);
        super.closeInputFormat();
    }

    /**
     * 获取hbase表级别的元数据信息
     *
     * @param tableName 表名
     * @return 表的元数据
     * @throws SQLException sql异常
     */
    protected HbaseTableEntity queryTableProperties(String tableName) throws SQLException {
        HbaseTableEntity hbaseTableEntity = new HbaseTableEntity();
        try {
            HTableDescriptor table = admin.getTableDescriptor(TableName.valueOf(tableName));
            List<HRegionInfo> regionInfos = admin.getTableRegions(table.getTableName());
            hbaseTableEntity.setRegionCount(regionInfos.size());
            //统一表大小单位为字节
            String tableSize = SizeUnitType.covertUnit(SizeUnitType.MB, SizeUnitType.B, Long.valueOf(tableSizeMap.get(table.getNameAsString())));
            hbaseTableEntity.setTotalSize(Long.valueOf(tableSize));
            hbaseTableEntity.setCreateTime(createTimeMap.get(table.getNameAsString()));
            //这里的table带了schema
            if (tableName.contains(ConstantValue.COLON_SYMBOL)) {
                tableName = tableName.split(ConstantValue.COLON_SYMBOL)[1];
            }
            hbaseTableEntity.setTableName(tableName);
            hbaseTableEntity.setNamespace(currentDatabase);
        } catch (IOException e) {
            LOG.error("query tableProperties failed. {}", ExceptionUtil.getErrorMessage(e));
            throw new SQLException(e);
        }
        return hbaseTableEntity;
    }

    /**
     * 获取列族信息
     *
     * @return 列族
     */
    protected List<HbaseColumnEntity> queryColumnList(String tableName) throws SQLException {
        List<HbaseColumnEntity> columnList = new ArrayList<>();
        try {
            HTableDescriptor table = admin.getTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor[] columnDescriptors = table.getColumnFamilies();
            for (HColumnDescriptor column : columnDescriptors) {
                HbaseColumnEntity hbaseColumnEntity = new HbaseColumnEntity();
                hbaseColumnEntity.setColumnFamily(column.getNameAsString());
                columnList.add(hbaseColumnEntity);
            }
        } catch (IOException e) {
            LOG.error("query columnList failed. {}", ExceptionUtil.getErrorMessage(e));
            throw new SQLException(e);
        }
        return columnList;
    }

    /**
     * 查询hbase表的创建时间
     * 如果zookeeper没有权限访问，返回空map
     *
     * @param hadoopConfig hadoop配置
     * @return 表名与创建时间的映射
     */
    protected Map<String, Long> queryCreateTimeMap(Map<String, Object> hadoopConfig) {
        Map<String, Long> createTimeMap = new HashMap<>(16);
        try {
            zooKeeper = ZkHelper.createZkClient((String) hadoopConfig.get(HConstants.ZOOKEEPER_QUORUM), ZkHelper.DEFAULT_TIMEOUT);
            List<String> tables = ZkHelper.getChildren(zooKeeper, path);
            if (tables != null) {
                for (String table : tables) {
                    LOG.info(table);
                    createTimeMap.put(table, ZkHelper.getCreateTime(zooKeeper, path + ConstantValue.SINGLE_SLASH_SYMBOL + table));
                }
            }
        } catch (Exception e) {
            LOG.error("query createTime map failed, error {} ", ExceptionUtil.getErrorMessage(e));
        } finally {
            ZkHelper.closeZooKeeper(zooKeeper);
        }
        return createTimeMap;
    }

    public void setPath(String path) {
        this.path = path + APPEND_PATH;
    }


    public void setHadoopConfig(Map<String, Object> hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
    }
}
