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

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.hdfs.conf.HdfsConf;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsRawTypeConverter;
import com.dtstack.chunjun.connector.hdfs.sink.BaseHdfsOutputFormat;
import com.dtstack.chunjun.connector.hdfs.sink.HdfsOutputFormatBuilder;
import com.dtstack.chunjun.connector.hdfs.util.HdfsUtil;
import com.dtstack.chunjun.connector.hive.conf.HiveConf;
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

import org.apache.commons.collections.MapUtils;
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

/**
 * Date: 2021/06/22 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HiveOutputFormat extends BaseRichOutputFormat {

    private org.apache.flink.configuration.Configuration parameters;
    private int taskNumber;
    private int numTasks;

    private HiveConf hiveConf;
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
        connectionInfo.setJdbcUrl(hiveConf.getJdbcUrl());
        connectionInfo.setUsername(hiveConf.getUsername());
        connectionInfo.setPassword(hiveConf.getPassword());
        connectionInfo.setHiveConf(hiveConf.getHadoopConfig());
        primaryCreateTable();
    }

    @Override
    public synchronized void writeRecord(RowData rowData) {
        if (RowKind.INSERT != rowData.getRowKind()) {
            throw new ChunJunRuntimeException("Hive connector doesn't support update/delete!");
        }
        String tableName = hiveConf.getTableName();
        boolean hasAnalyticalRules = StringUtils.isNotBlank(hiveConf.getAnalyticalRules());
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
                                    hiveConf.getAnalyticalRules(),
                                    hiveConf.getDistributeTableMapping());
                }
            } else {
                if (hasAnalyticalRules) {
                    tableName =
                            PathConverterUtil.regexByRules(
                                    columnRowData,
                                    hiveConf.getAnalyticalRules(),
                                    hiveConf.getDistributeTableMapping());
                }
            }
        }

        Pair<BaseHdfsOutputFormat, TableInfo> formatPair =
                getHdfsOutputFormat(tableName, rowData, dataMap);

        try {
            BaseHdfsOutputFormat hdfsOutputFormat = formatPair.getLeft();
            HdfsConf hdfsConf = hdfsOutputFormat.getHdfsConf();
            List<FieldConf> fieldConfList = hdfsConf.getColumn();
            RowData forwardRowData = null;
            if (dataMap != null) {
                ColumnRowData result = new ColumnRowData(fieldConfList.size());
                for (FieldConf fieldConf : fieldConfList) {
                    Object data = dataMap.get(fieldConf.getName());
                    result.addField(HiveUtil.parseDataFromMap(data));
                }
                forwardRowData = result;
            } else if (rowData instanceof ColumnRowData) {
                ColumnRowData columnRowData = (ColumnRowData) rowData;
                if (columnRowData.getHeaders() != null) {
                    ColumnRowData result = new ColumnRowData(fieldConfList.size());
                    for (FieldConf fieldConf : fieldConfList) {
                        AbstractBaseColumn baseColumn = columnRowData.getField(fieldConf.getName());
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
                LOG.warn("write hdfs exception:", e);
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
                LOG.warn("close {} outputFormat error", entry.getKey(), e);
            }
        }
    }

    private Pair<BaseHdfsOutputFormat, TableInfo> getHdfsOutputFormat(
            String tableName, RowData rowData, Map<String, Object> event) {
        String partitionValue = partitionFormat.format(new Date());
        String partitionPath =
                String.format(HiveUtil.PARTITION_TEMPLATE, hiveConf.getPartition(), partitionValue);
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
                    hiveConf.getSchema(),
                    partitionPath,
                    connectionInfo,
                    getRuntimeContext().getDistributedCache());
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
                    LOG.warn("close {} outputFormat error", hiveTablePath, e);
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
                    HdfsOutputFormatBuilder.newBuild(hiveConf.getFileType());
            HiveConf copyHiveConf =
                    GsonUtil.GSON.fromJson(GsonUtil.GSON.toJson(hiveConf), HiveConf.class);
            copyHiveConf.setPath(path);
            copyHiveConf.setFileName(null);
            List<String> columnNameList = tableInfo.getColumnNameList();
            List<String> columnTypeList = tableInfo.getColumnTypeList();
            List<FieldConf> fieldConfList = new ArrayList<>(columnNameList.size());
            for (int i = 0; i < columnNameList.size(); i++) {
                FieldConf fieldConf = new FieldConf();
                fieldConf.setIndex(i);
                fieldConf.setName(columnNameList.get(i));
                fieldConf.setType(columnTypeList.get(i));
                fieldConfList.add(fieldConf);
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
                            HdfsRawTypeConverter::apply),
                    useAbstractBaseColumn);
            builder.setInitAccumulatorAndDirty(false);

            BaseHdfsOutputFormat outputFormat = (BaseHdfsOutputFormat) builder.finish();
            outputFormat.setFormatId(hiveTablePath);
            outputFormat.setDirtyDataManager(dirtyDataManager);
            outputFormat.setRuntimeContext(getRuntimeContext());
            outputFormat.setRestoreState(formatStateMap.get(hiveTablePath));
            outputFormat.configure(parameters);
            outputFormat.open(taskNumber, numTasks);
            outputFormat.initializeGlobal(numTasks);

            return outputFormat;
        } catch (Exception e) {
            LOG.error("create [HdfsOutputFormat] exception:", e);
            throw new ChunJunRuntimeException(e);
        }
    }

    /** 预先建表 只适用于analyticalRules参数为schema和table的情况 */
    private void primaryCreateTable() {
        for (Map.Entry<String, TableInfo> entry : hiveConf.getTableInfos().entrySet()) {
            Map<String, Object> event = new HashMap<>(4);
            event.put("schema", hiveConf.getSchema());
            event.put("table", entry.getKey());
            TableInfo tableInfo = entry.getValue();
            String tablePath =
                    PathConverterUtil.regexByRules(
                            event, hiveConf.getTableName(), hiveConf.getDistributeTableMapping());
            tableInfo.setTablePath(tablePath);
            checkCreateTable(tablePath, null, event);
        }
    }

    private TableInfo checkCreateTable(
            String tablePath, RowData rowData, Map<String, Object> event) {
        TableInfo tableInfo = tableCacheMap.get(tablePath);
        if (tableInfo == null) {
            LOG.info("tablePath:{}, rowData:{}, even:{}", tablePath, rowData, event);

            String tableName = tablePath;
            if (event != null) {
                tableName = MapUtils.getString(event, "table");
                tableName = hiveConf.getDistributeTableMapping().getOrDefault(tableName, tableName);
            } else if (rowData instanceof ColumnRowData) {
                ColumnRowData columnRowData = (ColumnRowData) rowData;
                AbstractBaseColumn baseColumn = columnRowData.getField("table");
                if (baseColumn != null) {
                    tableName = baseColumn.asString();
                    tableName =
                            hiveConf.getDistributeTableMapping().getOrDefault(tableName, tableName);
                }
            }
            tableInfo = hiveConf.getTableInfos().get(tableName);
            if (tableInfo == null) {
                throw new ChunJunRuntimeException(
                        "tableName:" + tableName + " of the tableInfo is null");
            }
            tableInfo.setTablePath(tablePath);
            HiveUtil.createHiveTableWithTableInfo(
                    tableInfo,
                    hiveConf.getSchema(),
                    connectionInfo,
                    getRuntimeContext().getDistributedCache());
            tableCacheMap.put(tablePath, tableInfo);
        }
        return tableInfo;
    }

    private SimpleDateFormat getPartitionFormat() {
        if (StringUtils.isBlank(hiveConf.getPartitionType())) {
            throw new IllegalArgumentException("partitionEnumStr is empty!");
        }
        SimpleDateFormat format;
        switch (hiveConf.getPartitionType().toUpperCase(Locale.ENGLISH)) {
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
                        "partitionEnum = " + hiveConf.getPartitionType() + " is undefined!");
        }
        TimeZone timeZone = TimeZone.getDefault();
        LOG.info("timeZone = {}", timeZone);
        format.setTimeZone(timeZone);
        return format;
    }

    public void setHiveConf(HiveConf hiveConf) {
        this.hiveConf = hiveConf;
    }

    public HiveConf getHiveConf() {
        return hiveConf;
    }
}
