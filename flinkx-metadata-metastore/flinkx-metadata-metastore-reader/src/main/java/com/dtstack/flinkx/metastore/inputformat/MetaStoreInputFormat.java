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
package com.dtstack.flinkx.metastore.inputformat;

import com.dtstack.flinkx.metadata.core.entity.MetadataEntity;
import com.dtstack.flinkx.metadata.inputformat.MetadataBaseInputFormat;
import com.dtstack.flinkx.metastore.constants.MetaStoreClientUtil;
import com.dtstack.flinkx.metastore.entity.MetaStoreClientInfo;
import com.dtstack.flinkx.metastore.constants.MetaDataCons;
import com.dtstack.flinkx.metatdata.hive2.core.entity.HiveTableEntity;
import com.dtstack.flinkx.metatdata.hive2.core.entity.MetadataHive2Entity;
import com.dtstack.flinkx.metatdata.hive2.core.util.HiveOperatorUtils;
import com.dtstack.metadata.rdb.core.entity.ColumnEntity;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;


import static com.dtstack.flinkx.metatdata.hive2.core.util.Hive2MetaDataCons.KEY_COMMENT;
import static com.dtstack.flinkx.metatdata.hive2.core.util.Hive2MetaDataCons.KEY_TOTALSIZE;
import static com.dtstack.flinkx.metatdata.hive2.core.util.Hive2MetaDataCons.KEY_TRANSIENT_LASTDDLTIME;


/**
 * @author : tiezhu
 * @date : 2020/3/9
 */
public class MetaStoreInputFormat extends MetadataBaseInputFormat {


    private static final long serialVersionUID = 1L;

    /**集群配置信息*/
    protected Map<String, Object> hadoopConfig;

    /**metastore客户端*/
    private HiveMetaStoreClient hiveMetaStoreClient;

    private static Logger LOG = LoggerFactory.getLogger(MetaStoreInputFormat.class);

    public void setHadoopConfig(Map<String, Object> hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
    }

    @Override
    protected void doOpenInternal() throws IOException {
        if(hiveMetaStoreClient == null){
            LOG.info("start init hive metaStore client");
            String metaStoreUrl = String.valueOf(hadoopConfig.get(MetaDataCons.META_STORE));
            hiveMetaStoreClient = MetaStoreClientUtil.getClient(new MetaStoreClientInfo(metaStoreUrl, hadoopConfig));
        }
        if(CollectionUtils.isEmpty(tableList)){
            tableList = showTables();
        }
        LOG.info("finish init hive metaStore client");
    }



    /**
     * 当传入表名为空时，手动查询所有表
     * 提供默认实现为只查询表名的情况，查询
     * @return 表名
     */
    public List<Object> showTables() {
        List<Object> tables = Lists.newArrayList();
        try {
            tables.addAll(hiveMetaStoreClient.getAllTables(currentDatabase));
        } catch (TException e) {
            throw new RuntimeException("获取表列表失败", e);
        }
        return tables;
    }


    @Override
    protected void closeInternal() throws IOException {

    }

    @Override
    public void closeInputFormat() throws IOException {
        super.closeInputFormat();
        if (hiveMetaStoreClient != null) {
            hiveMetaStoreClient.close();
        }
    }

    @Override
    public MetadataEntity createMetadataEntity() throws Exception {
        MetadataHive2Entity metadataEntity = new MetadataHive2Entity();
        metadataEntity.setSchema(currentDatabase);
        metadataEntity.setTableName((String) currentObject);
        //获取hive表相关信息
        LOG.info("current database is {},table is {}", currentDatabase, currentObject);
        Table table = hiveMetaStoreClient.getTable(currentDatabase, (String) currentObject);
        List<ColumnEntity> hiveColumnEntities = transferColumn(table.getPartitionKeys());
        List<Partition> partitions = hiveMetaStoreClient.listPartitions(currentDatabase, (String) currentObject, Short.MAX_VALUE);
        metadataEntity.setPartitionColumns(hiveColumnEntities);
        metadataEntity.setColumns(transferColumn(table.getSd().getCols()));
        metadataEntity.setPartitions(getPartitionDetail(hiveColumnEntities, partitions));
        //TODO sparkserver 元数据表大小和行数，分区文件数量不准确，只支持hiverserver
        HiveTableEntity hiveTableEntity = generateTableInfo(table);
        //组装所有分区的大小和行数
        Long rows = 0L;
        Long dataSize = 0L;
        for (Partition partition : partitions) {
            Map<String, String> paramMap = partition.getParameters();
            Long row = MapUtils.getLong(paramMap, MetaDataCons.NUM_ROWS);
            rows += (row == null ? 0 : row);
            dataSize += MapUtils.getLong(paramMap, KEY_TOTALSIZE);
        }
        hiveTableEntity.setTotalSize(dataSize);
        hiveTableEntity.setRows(rows);
        metadataEntity.setTableProperties(hiveTableEntity);
        return metadataEntity;
    }


    /**
     * 表参数转化
     *
     * @param table
     * @return
     */
    private HiveTableEntity generateTableInfo(Table table) {
        HiveTableEntity hiveTableProperties = new HiveTableEntity();
        hiveTableProperties.setComment(table.getParameters().get(KEY_COMMENT));
        hiveTableProperties.setLocation(table.getSd().getLocation());
        hiveTableProperties.setCreateTime(String.valueOf(table.getCreateTime()));
        hiveTableProperties.setLastAccessTime(String.valueOf(table.getLastAccessTime()));
        hiveTableProperties.setTransientLastDdlTime(table.getParameters().get(KEY_TRANSIENT_LASTDDLTIME));
        hiveTableProperties.setStoreType(HiveOperatorUtils.getStoredType(table.getSd().getOutputFormat()));
        hiveTableProperties.setTableName(table.getTableName());
        return hiveTableProperties;
    }


    /**
     * 字段属性转化
     *
     * @param cols
     * @return
     */
    private List<ColumnEntity> transferColumn(List<FieldSchema> cols) {
        List<ColumnEntity> hiveColumnEntities = Lists.newArrayList();
        if (CollectionUtils.isEmpty(cols)) {
            return hiveColumnEntities;
        }
        int fieldIndex = 1;
        for (FieldSchema fieldSchema : cols) {
            ColumnEntity hiveColumnEntity = new ColumnEntity();
            hiveColumnEntity.setType(fieldSchema.getType());
            hiveColumnEntity.setName(fieldSchema.getName());
            hiveColumnEntity.setComment(fieldSchema.getComment());
            hiveColumnEntity.setIndex(fieldIndex);
            fieldIndex++;
            hiveColumnEntities.add(hiveColumnEntity);
        }
        return hiveColumnEntities;
    }

    private List<String> getPartitionDetail(List<ColumnEntity> hiveColumnEntities, List<Partition> partitions) {
        List<String> partitionLists = Lists.newArrayList();
        if (CollectionUtils.isEmpty(hiveColumnEntities)) {
            return partitionLists;
        }
        partitions.forEach(partition -> {
            List<String> partitionValues = partition.getValues();
            partitionLists.add(assemblePartition(partitionValues, hiveColumnEntities));
        });
        return partitionLists;
    }

    private String assemblePartition(List<String> partitionValues, List<ColumnEntity> hiveColumnEntities) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < partitionValues.size(); i++) {
            str.append(hiveColumnEntities.get(i).getName()).append("=").append(partitionValues.get(i)).append(",");
        }
        return str.substring(0, str.toString().length() - 1);
    }


}
