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
package com.dtstack.flinkx.hive.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.hive.TableInfo;
import com.dtstack.flinkx.hive.TimePartitionFormat;
import com.dtstack.flinkx.hive.util.HiveUtil;
import com.dtstack.flinkx.writer.BaseDataWriter;
import com.dtstack.flinkx.writer.WriteMode;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;
import parquet.hadoop.ParquetWriter;

import java.util.*;

import static com.dtstack.flinkx.hdfs.HdfsConfigKeys.KEY_ROW_GROUP_SIZE;
import static com.dtstack.flinkx.hive.HiveConfigKeys.*;
import static com.dtstack.flinkx.util.GsonUtil.GSON;

/**
 * @author toutian
 */
public class HiveWriter extends BaseDataWriter {

    private String readerName;

    private String defaultFs;

    private String fileType;

    private String partitionType;

    private String partition;

    private String delimiter;

    private String compress;

    private long bufferSize;

    private Map<String, TableInfo> tableInfos;

    private Map<String, String> distributeTableMapping;

    private Map<String, Object> hadoopConfig;

    private String charSet;

    private long maxFileSize;

    private int rowGroupSize;

    private String jdbcUrl;

    private String username;

    private String password;

    private String tableBasePath;

    private boolean autoCreateTable;

    private String schema;

    public HiveWriter(DataTransferConfig config) {
        super(config);
        readerName = config.getJob().getContent().get(0).getReader().getName();
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        hadoopConfig = (Map<String, Object>) writerConfig.getParameter().getVal(KEY_HADOOP_CONFIG);
        defaultFs = writerConfig.getParameter().getStringVal(KEY_DEFAULT_FS);
        if (StringUtils.isBlank(defaultFs) && hadoopConfig.containsKey(KEY_FS_DEFAULT_FS)){
            defaultFs = MapUtils.getString(hadoopConfig, KEY_FS_DEFAULT_FS);
        }
        fileType = writerConfig.getParameter().getStringVal(KEY_FILE_TYPE);
        partitionType = writerConfig.getParameter().getStringVal(KEY_PARTITION_TYPE, TimePartitionFormat.PartitionEnum.DAY.name());
        partition = writerConfig.getParameter().getStringVal(KEY_PARTITION, "pt");
        delimiter = writerConfig.getParameter().getStringVal(KEY_FIELD_DELIMITER, "\u0001");
        charSet = writerConfig.getParameter().getStringVal(KEY_CHARSET_NAME);
        maxFileSize = writerConfig.getParameter().getLongVal(KEY_MAX_FILE_SIZE, ConstantValue.STORE_SIZE_G);
        compress = writerConfig.getParameter().getStringVal(KEY_COMPRESS);
        bufferSize = writerConfig.getParameter().getLongVal(KEY_BUFFER_SIZE, 128 * ConstantValue.STORE_SIZE_M);
        rowGroupSize = writerConfig.getParameter().getIntVal(KEY_ROW_GROUP_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE);

        mode = writerConfig.getParameter().getStringVal(KEY_WRITE_MODE, WriteMode.APPEND.name());
        jdbcUrl = writerConfig.getParameter().getStringVal(KEY_JDBC_URL);
        username = writerConfig.getParameter().getStringVal(KEY_USERNAME);
        password = writerConfig.getParameter().getStringVal(KEY_PASSWORD);
        schema = writerConfig.getParameter().getStringVal(KEY_SCHEMA);

        String distributeTable = writerConfig.getParameter().getStringVal(KEY_DISTRIBUTE_TABLE);
        formatHiveDistributeInfo(distributeTable);

        String tablesColumn = writerConfig.getParameter().getStringVal(KEY_TABLE_COLUMN);
        formatHiveTableInfo(tablesColumn);

        String analyticalRules = writerConfig.getParameter().getStringVal(KEY_ANALYTICAL_RULES);
        if (StringUtils.isBlank(analyticalRules)) {
            tableBasePath = tableInfos.entrySet().iterator().next().getValue().getTableName();
        } else {
            tableBasePath = analyticalRules;
            autoCreateTable = true;
        }
    }

    /**
     * 分表的映射关系
     * distributeTableMapping 的数据结构为<tableName,groupName>
     * tableInfos的数据结构为<groupName,TableInfo>
     */
    private void formatHiveDistributeInfo(String distributeTable) {
        distributeTableMapping = new HashMap<>(32);
        if (StringUtils.isNotBlank(distributeTable)) {
            Map<String, List<String>> distributeTableMap = GSON.fromJson(distributeTable, new TypeToken<TreeMap<String, List<String>>>(){}.getType());
            for (Map.Entry<String, List<String>> entry : distributeTableMap.entrySet()) {
                String groupName = entry.getKey();
                List<String> groupTables = entry.getValue();
                for (String tableName : groupTables) {
                    distributeTableMapping.put(tableName, groupName);
                }
            }
        }
    }

    private void formatHiveTableInfo(String tablesColumn) {
        tableInfos = new HashMap<>(16);
        if (StringUtils.isNotEmpty(tablesColumn)) {
            Map<String, List<Map<String, Object>>>  tableColumnMap = GSON.fromJson(tablesColumn, new TypeToken<TreeMap<String, List<Map<String, Object>> >>(){}.getType());
            List<Map<String, Object>> extraTableColumnList = getExtraTableColumn();
            for (Map.Entry<String, List<Map<String, Object>>> entry : tableColumnMap.entrySet()) {
                String tableName = entry.getKey();
                List<Map<String, Object>> tableColumns = entry.getValue();
                tableColumns.addAll(extraTableColumnList);
                TableInfo tableInfo = new TableInfo(tableColumns.size());
                tableInfo.addPartition(partition);
                tableInfo.setDelimiter(delimiter);
                tableInfo.setStore(fileType);
                tableInfo.setTableName(tableName);
                for (Map<String, Object> column : tableColumns) {
                    tableInfo.addColumnAndType(MapUtils.getString(column, HiveUtil.TABLE_COLUMN_KEY), HiveUtil.getHiveColumnType(MapUtils.getString(column, HiveUtil.TABLE_COLUMN_TYPE)));
                }
                String createTableSql = HiveUtil.getCreateTableHql(tableInfo);
                tableInfo.setCreateTableSql(createTableSql);

                tableInfos.put(tableName, tableInfo);
            }
        }
    }

    /**
     * 增加hive表字段
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getExtraTableColumn(){
        if(StringUtils.equalsIgnoreCase(readerName, "oraclelogminerreader")){
            List<Map<String, Object>> list = new ArrayList<>(2);
            Map<String, Object> opTime = new LinkedTreeMap<>();
            opTime.put("type", "BIGINT");
            opTime.put("key", "opTime");
            opTime.put("comment", "");

            Map<String, Object> scn = new LinkedTreeMap<>();
            scn.put("type", "BIGINT");
            scn.put("key", "scn");
            scn.put("comment", "");

            list.add(opTime);
            list.add(scn);

            return list;
        }else{
            return Collections.EMPTY_LIST;
        }
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        HiveOutputFormatBuilder builder = new HiveOutputFormatBuilder();
        builder.setHadoopConfig(hadoopConfig);
        builder.setDefaultFs(defaultFs);
        builder.setWriteMode(mode);
        builder.setCompress(compress);
        builder.setCharSetName(charSet);
        builder.setMaxFileSize(maxFileSize);
        builder.setRowGroupSize(rowGroupSize);
        builder.setFileType(fileType);
        builder.setDelimiter(delimiter);

        builder.setSchema(schema);
        builder.setPartition(partition);
        builder.setPartitionType(partitionType);
        builder.setBufferSize(bufferSize);
        builder.setJdbcUrl(jdbcUrl);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setTableBasePath(tableBasePath);
        builder.setAutoCreateTable(autoCreateTable);

        builder.setDistributeTableMapping(distributeTableMapping);
        builder.setTableInfos(tableInfos);

        builder.setMonitorUrls(monitorUrls);
        builder.setErrors(errors);
        builder.setErrorRatio(errorRatio);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);

        builder.setRestoreConfig(restoreConfig);

        return createOutput(dataSet, builder.finish());
    }
}
