/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.hive.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.hdfs.config.HdfsConfig;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsRawTypeMapper;
import com.dtstack.chunjun.connector.hdfs.sink.BaseHdfsOutputFormat;
import com.dtstack.chunjun.connector.hdfs.sink.HdfsOutputFormatBuilder;
import com.dtstack.chunjun.connector.hdfs.util.HdfsUtil;
import com.dtstack.chunjun.connector.hive.config.HiveConfig;
import com.dtstack.chunjun.connector.hive.entity.ConnectionInfo;
import com.dtstack.chunjun.connector.hive.entity.HiveFormatState;
import com.dtstack.chunjun.connector.hive.entity.TableInfo;
import com.dtstack.chunjun.connector.hive.util.HiveUtil;
import com.dtstack.chunjun.connector.hive.util.PathConverterUtil;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.MapColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.enums.Semantic;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

@Slf4j
public class HiveOutputFormat extends BaseRichOutputFormat {

    private static final long serialVersionUID = -5345705130137076697L;

    private org.apache.flink.configuration.Configuration parameters;
    private int taskNumber;
    private int numTasks;

    private HiveConfig hiveConfig;
    private ConnectionInfo connectionInfo;
    private SimpleDateFormat partitionFormat;

    private Map<String, TableInfo> tableCacheMap;
    private Map<String, Pair<String, BaseHdfsOutputFormat>> outputFormatMap;
    private Map<String, FormatState> formatStateMap;

    @Override
    public void configure(org.apache.flink.configuration.Configuration parameters) {
        super.configure(parameters);
        this.parameters = parameters;

        partitionFormat = getPartitionFormat();
        tableCacheMap = new HashMap<>(16);
        outputFormatMap = new HashMap<>(16);
        formatStateMap = new HashMap<>(16);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        super.checkpointMode = CheckpointingMode.EXACTLY_ONCE;
        super.semantic = Semantic.EXACTLY_ONCE;
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;

        if (null != formatState && null != formatState.getState()) {
            HiveFormatState hiveFormatState = (HiveFormatState) formatState.getState();
            formatStateMap.putAll(hiveFormatState.getFormatStateMap());
        }

        connectionInfo = new ConnectionInfo();
        connectionInfo.setJdbcUrl(hiveConfig.getJdbcUrl());
        connectionInfo.setUsername(hiveConfig.getUsername());
        connectionInfo.setPassword(hiveConfig.getPassword());
        connectionInfo.setHiveConfig(hiveConfig.getHadoopConfig());
        primaryCreateTable();
    }

    @Override
    public synchronized void writeRecord(RowData rowData) {
        if (RowKind.INSERT != rowData.getRowKind()) {
            throw new ChunJunRuntimeException("Hive connector doesn't support update/delete!");
        }
        String tableName = hiveConfig.getTableName();
        boolean hasAnalyticalRules = StringUtils.isNotBlank(hiveConfig.getAnalyticalRules());
        Map<String, Object> dataMap = null;
        if (rowData instanceof ColumnRowData) {
            ColumnRowData columnRowData = (ColumnRowData) rowData;
            AbstractBaseColumn baseColumn = (columnRowData).getField(0);
            if (baseColumn instanceof MapColumn) {
                // from kafka
                dataMap = JsonUtil.toObject(baseColumn.asString(), JsonUtil.MAP_TYPE_REFERENCE);
                if (hasAnalyticalRules) {
                    tableName =
                            PathConverterUtil.regexByRules(
                                    dataMap,
                                    hiveConfig.getAnalyticalRules(),
                                    hiveConfig.getDistributeTableMapping());
                }
            } else {
                if (hasAnalyticalRules) {
                    tableName =
                            PathConverterUtil.regexByRules(
                                    columnRowData,
                                    hiveConfig.getAnalyticalRules(),
                                    hiveConfig.getDistributeTableMapping());
                }
            }
        }

        Pair<BaseHdfsOutputFormat, TableInfo> formatPair =
                getHdfsOutputFormat(tableName, rowData, dataMap);

        try {
            BaseHdfsOutputFormat hdfsOutputFormat = formatPair.getLeft();
            HdfsConfig hdfsConfig = hdfsOutputFormat.getHdfsConf();
            List<FieldConfig> fieldConfList = hdfsConfig.getColumn();
            RowData forwardRowData = null;
            if (dataMap != null) {
                ColumnRowData result = new ColumnRowData(fieldConfList.size());
                for (FieldConfig fieldConfig : fieldConfList) {
                    Object data = dataMap.get(fieldConfig.getName());
                    result.addField(HiveUtil.parseDataFromMap(data));
                }
                forwardRowData = result;
            } else if (rowData instanceof ColumnRowData) {
                ColumnRowData columnRowData = (ColumnRowData) rowData;
                if (columnRowData.getHeaders() != null) {
                    ColumnRowData result = new ColumnRowData(fieldConfList.size());
                    for (FieldConfig fieldConfig : fieldConfList) {
                        AbstractBaseColumn baseColumn =
                                columnRowData.getField(fieldConfig.getName());
                        if (baseColumn != null) {
                            result.addField(baseColumn);
                        } else {
                            result.addField(new NullColumn());
                        }
                    }
                    forwardRowData = result;
                }
            }
            if (forwardRowData == null) {
                forwardRowData = rowData;
            }

            hdfsOutputFormat.writeRecord(forwardRowData);
        } catch (Exception e) {
            // 写入产生的脏数据已经由hdfsOutputFormat处理了，这里不用再处理了，只打印日志
            if (numWriteCounter.getLocalValue() % LOG_PRINT_INTERNAL == 0) {
                log.warn("write hdfs exception:", e);
            }
        }
        rowsOfCurrentTransaction++;
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) {
        throw new ChunJunRuntimeException(
                "method[writeSingleRecordInternal] in HiveOutputFormat should not be invoked, this is code error.");
    }

    @Override
    protected void writeMultipleRecordsInternal() {
        throw new ChunJunRuntimeException(
                "method[writeMultipleRecordsInternal] in HiveOutputFormat should not be invoked, this is code error.");
    }

    @Override
    public synchronized FormatState getFormatState() throws Exception {
        formatStateMap.clear();
        for (Map.Entry<String, Pair<String, BaseHdfsOutputFormat>> next :
                outputFormatMap.entrySet()) {
            BaseHdfsOutputFormat format = next.getValue().getRight();
            FormatState formatState = format.getFormatState();
            formatStateMap.put(format.getFormatId(), formatState);
        }

        // set metric after preCommit
        snapshotWriteCounter.add(rowsOfCurrentTransaction);
        rowsOfCurrentTransaction = 0;
        formatState.setNumberWrite(numWriteCounter.getLocalValue());
        formatState.setMetric(outputMetric.getMetricCounters());
        formatState.setState(new HiveFormatState(formatStateMap));
        flushEnable.compareAndSet(true, false);
        return formatState;
    }

    @Override
    public void commit(long checkpointId) {
        for (Map.Entry<String, Pair<String, BaseHdfsOutputFormat>> next :
                outputFormatMap.entrySet()) {
            BaseHdfsOutputFormat format = next.getValue().getRight();
            format.commit(checkpointId);
        }
    }

    @Override
    public void rollback(long checkpointId) {
        for (Map.Entry<String, Pair<String, BaseHdfsOutputFormat>> next :
                outputFormatMap.entrySet()) {
            BaseHdfsOutputFormat format = next.getValue().getRight();
            format.rollback(checkpointId);
        }
    }

    @Override
    public void closeInternal() {
        for (Map.Entry<String, Pair<String, BaseHdfsOutputFormat>> entry :
                outputFormatMap.entrySet()) {
            try {
                BaseHdfsOutputFormat format = entry.getValue().getRight();
                format.close();
                format.finalizeGlobal(numTasks);
            } catch (IOException e) {
                log.warn("close {} outputFormat error", entry.getKey(), e);
            }
        }
    }

    private Pair<BaseHdfsOutputFormat, TableInfo> getHdfsOutputFormat(
            String tableName, RowData rowData, Map<String, Object> event) {
        String partitionValue = partitionFormat.format(new Date());
        String partitionPath =
                String.format(
                        HiveUtil.PARTITION_TEMPLATE, hiveConfig.getPartition(), partitionValue);
        String hiveTablePath = tableName + File.separatorChar + partitionPath;

        Pair<String, BaseHdfsOutputFormat> formatPair = outputFormatMap.get(tableName);
        BaseHdfsOutputFormat outputFormat = null;
        if (formatPair != null && StringUtils.equals(formatPair.getLeft(), partitionPath)) {
            outputFormat = formatPair.getRight();
        }
        TableInfo tableInfo = checkCreateTable(tableName, rowData, event);
        if (outputFormat == null) {
            HiveUtil.createPartition(
                    tableInfo,
                    hiveConfig.getSchema(),
                    partitionPath,
                    connectionInfo,
                    getRuntimeContext().getDistributedCache(),
                    jobId,
                    String.valueOf(taskNumber));
            String path = tableInfo.getPath() + File.separatorChar + partitionPath;

            outputFormat =
                    createHdfsOutputFormat(
                            tableInfo, path, hiveTablePath, rowData instanceof ColumnRowData);
            if (formatPair != null) {
                try {
                    BaseHdfsOutputFormat format = formatPair.getRight();
                    format.finalizeGlobal(numTasks);
                    format.close();
                } catch (IOException e) {
                    log.warn("close {} outputFormat error", hiveTablePath, e);
                }
            }
            outputFormatMap.put(tableName, Pair.of(partitionPath, outputFormat));
        }
        return Pair.of(outputFormat, tableInfo);
    }

    private BaseHdfsOutputFormat createHdfsOutputFormat(
            TableInfo tableInfo, String path, String hiveTablePath, boolean useAbstractBaseColumn) {
        try {
            HdfsOutputFormatBuilder builder =
                    HdfsOutputFormatBuilder.newBuild(hiveConfig.getFileType());
            HiveConfig copyHiveConf =
                    GsonUtil.GSON.fromJson(GsonUtil.GSON.toJson(hiveConfig), HiveConfig.class);
            copyHiveConf.setPath(path);
            copyHiveConf.setFileName(null);
            List<String> columnNameList = tableInfo.getColumnNameList();
            List<String> columnTypeList = tableInfo.getColumnTypeList();
            List<FieldConfig> fieldConfList = new ArrayList<>(columnNameList.size());
            for (int i = 0; i < columnNameList.size(); i++) {
                FieldConfig fieldConfig = new FieldConfig();
                fieldConfig.setIndex(i);
                fieldConfig.setName(columnNameList.get(i));
                fieldConfig.setType(TypeConfig.fromString(columnTypeList.get(i)));
                fieldConfList.add(fieldConfig);
            }
            copyHiveConf.setColumn(fieldConfList);
            copyHiveConf.setFullColumnName(columnNameList);
            copyHiveConf.setFullColumnType(columnTypeList);
            builder.setHdfsConf(copyHiveConf);

            builder.setRowConverter(
                    HdfsUtil.createRowConverter(
                            useAbstractBaseColumn,
                            copyHiveConf.getFileType(),
                            fieldConfList,
                            HdfsRawTypeMapper::apply,
                            hiveConfig),
                    useAbstractBaseColumn);
            builder.setInitAccumulatorAndDirty(false);

            BaseHdfsOutputFormat outputFormat = (BaseHdfsOutputFormat) builder.finish();
            outputFormat.setFormatId(hiveTablePath);
            outputFormat.setRuntimeContext(getRuntimeContext());
            outputFormat.setRestoreState(formatStateMap.get(hiveTablePath));
            outputFormat.configure(parameters);
            outputFormat.open(taskNumber, numTasks);
            outputFormat.initializeGlobal(numTasks);

            return outputFormat;
        } catch (Exception e) {
            log.error("create [HdfsOutputFormat] exception:", e);
            throw new ChunJunRuntimeException(e);
        }
    }

    /** 预先建表 只适用于analyticalRules参数为schema和table的情况 */
    private void primaryCreateTable() {
        for (Map.Entry<String, TableInfo> entry : hiveConfig.getTableInfos().entrySet()) {
            Map<String, Object> event = new HashMap<>(4);
            event.put("schema", hiveConfig.getSchema());
            event.put("table", entry.getKey());
            TableInfo tableInfo = entry.getValue();
            String tablePath =
                    PathConverterUtil.regexByRules(
                            event,
                            hiveConfig.getTableName(),
                            hiveConfig.getDistributeTableMapping());
            tableInfo.setTablePath(tablePath);
            checkCreateTable(tablePath, null, event);
        }
    }

    private TableInfo checkCreateTable(
            String tablePath, RowData rowData, Map<String, Object> event) {
        TableInfo tableInfo = tableCacheMap.get(tablePath);
        if (tableInfo == null) {
            log.info("tablePath:{}, rowData:{}, even:{}", tablePath, rowData, event);

            String tableName = tablePath;
            if (event != null) {
                tableName = MapUtils.getString(event, "table");
                tableName =
                        hiveConfig.getDistributeTableMapping().getOrDefault(tableName, tableName);
            } else if (rowData instanceof ColumnRowData) {
                ColumnRowData columnRowData = (ColumnRowData) rowData;
                AbstractBaseColumn baseColumn = columnRowData.getField("table");
                if (baseColumn != null) {
                    tableName = baseColumn.asString();
                    tableName =
                            hiveConfig
                                    .getDistributeTableMapping()
                                    .getOrDefault(tableName, tableName);
                }
            }
            tableInfo = hiveConfig.getTableInfos().get(tableName);
            if (tableInfo == null) {
                throw new ChunJunRuntimeException(
                        "tableName:" + tableName + " of the tableInfo is null");
            }
            tableInfo.setTablePath(tablePath);
            HiveUtil.createHiveTableWithTableInfo(
                    tableInfo,
                    hiveConfig.getSchema(),
                    connectionInfo,
                    getRuntimeContext().getDistributedCache(),
                    jobId,
                    String.valueOf(taskNumber));
            tableCacheMap.put(tablePath, tableInfo);
        }
        return tableInfo;
    }

    private SimpleDateFormat getPartitionFormat() {
        if (StringUtils.isBlank(hiveConfig.getPartitionType())) {
            throw new IllegalArgumentException("partitionEnumStr is empty!");
        }
        SimpleDateFormat format;
        switch (hiveConfig.getPartitionType().toUpperCase(Locale.ENGLISH)) {
            case "DAY":
                format = new SimpleDateFormat("yyyyMMdd");
                break;
            case "HOUR":
                format = new SimpleDateFormat("yyyyMMddHH");
                break;
            case "MINUTE":
                format = new SimpleDateFormat("yyyyMMddHHmm");
                break;
            default:
                throw new UnsupportedOperationException(
                        "partitionEnum = " + hiveConfig.getPartitionType() + " is undefined!");
        }
        TimeZone timeZone = TimeZone.getDefault();
        log.info("timeZone = {}", timeZone);
        format.setTimeZone(timeZone);
        return format;
    }

    public void setHiveConfig(HiveConfig hiveConfig) {
        this.hiveConfig = hiveConfig;
    }

    public HiveConfig getHiveConfig() {
        return hiveConfig;
    }
}
