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

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.hdfs.writer.BaseHdfsOutputFormat;
import com.dtstack.flinkx.hdfs.writer.HdfsOutputFormatBuilder;
import com.dtstack.flinkx.hive.TableInfo;
import com.dtstack.flinkx.hive.TimePartitionFormat;
import com.dtstack.flinkx.hive.util.HiveDbUtil;
import com.dtstack.flinkx.hive.util.HiveUtil;
import com.dtstack.flinkx.hive.util.PathConverterUtil;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author toutian
 */
public class HiveOutputFormat extends BaseRichOutputFormat {

    private static final Logger logger = LoggerFactory.getLogger(HiveOutputFormat.class);

    private static final String SP = "/";

    /**
     * hdfs高可用配置
     */
    protected Map<String, Object> hadoopConfig;

    protected String fileType;

    /**
     * 写入模式
     */
    protected String writeMode;

    /**
     * 压缩方式
     */
    protected String compress;

    protected String defaultFs;

    protected String delimiter;

    protected String charsetName = "UTF-8";

    protected Configuration conf;

    protected int rowGroupSize;

    protected long maxFileSize;

    /* ----------以上hdfs插件参数----------- */

    protected Map<String, TableInfo> tableInfos;
    protected Map<String, String> distributeTableMapping;
    protected String partition;
    protected String partitionType;
    protected long bufferSize;
    protected String jdbcUrl;
    protected String username;
    protected String password;
    protected String tableBasePath;
    protected boolean autoCreateTable;

    private transient HiveUtil hiveUtil;
    private transient TimePartitionFormat partitionFormat;

    private org.apache.flink.configuration.Configuration parameters;
    private int taskNumber;
    private int numTasks;

    private Map<String, TableInfo> tableCache;
    private Map<String, BaseHdfsOutputFormat> outputFormats;

    private Map<String, FormatState> formatStateMap = new HashMap<>();

    @Override
    public void configure(org.apache.flink.configuration.Configuration parameters) {
        this.parameters = parameters;

        partitionFormat = TimePartitionFormat.getInstance(partitionType);
        tableCache = new HashMap<>(16);
        outputFormats = new HashMap<>(16);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;

        if (null != formatState && null != formatState.getState()) {
            HiveFormatState hiveFormatState = (HiveFormatState)formatState.getState();
            formatStateMap.putAll(hiveFormatState.getFormatStateMap());
        }

        HiveDbUtil.ConnectionInfo connectionInfo = new HiveDbUtil.ConnectionInfo();
        connectionInfo.setJdbcUrl(jdbcUrl);
        connectionInfo.setUsername(username);
        connectionInfo.setPassword(password);
        connectionInfo.setHiveConf(hadoopConfig);

        hiveUtil = new HiveUtil(connectionInfo);
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
    }

    @Override
    public FormatState getFormatState() {
        if (!restoreConfig.isRestore()) {
            LOG.info("return null for formatState");
            return null;
        }

        Map<String, FormatState> formatStateMap = flushOutputFormat();

        HiveFormatState hiveFormatState = new HiveFormatState(formatStateMap);
        formatState.setState(hiveFormatState);

        super.getFormatState();
        return formatState;
    }

    private Map<String, FormatState> flushOutputFormat() {
        Map<String, FormatState> formatStateMap = new HashMap<>();
        Iterator<Map.Entry<String, BaseHdfsOutputFormat>> entryIterator = outputFormats.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, BaseHdfsOutputFormat> entry = entryIterator.next();
            FormatState formatState = entry.getValue().getFormatState();
            formatStateMap.put(entry.getValue().getFormatId(), formatState);

            if (partitionFormat.isTimeout(entry.getValue().getLastWriteTime())) {
                try {
                    entry.getValue().close();
                } catch (Exception e) {
                    logger.error(ExceptionUtil.getErrorMessage(e));
                } finally {
                    entryIterator.remove();
                }
            }
        }

        return formatStateMap;
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
    }

    @Override
    public void writeRecord(Row row) throws IOException {
        boolean fromLogData = false;
        String tablePath;
        Map event = null;
        if (row.getField(0) instanceof Map) {
            event = (Map) row.getField(0);
            tablePath = PathConverterUtil.regaxByRules(event, tableBasePath, distributeTableMapping);
            fromLogData = true;
        } else {
            tablePath = tableBasePath;
        }

        Pair<BaseHdfsOutputFormat, TableInfo> formatPair;
        try {
            formatPair = getHdfsOutputFormat(tablePath, event);
        } catch (Exception e) {
            throw new RuntimeException("获取HDFSOutputFormat失败", e);
        }

        Row rowData = row;
        if (fromLogData) {
            rowData = setChannelInformation(event, row.getField(1), formatPair.getSecond().getColumns());
        }

        try {
            formatPair.getFirst().writeRecord(rowData);

            //row包含map嵌套的数据内容和channel， 而rowData是非常简单的纯数据，此处补上数据差额
            if (fromLogData && bytesWriteCounter != null) {
                bytesWriteCounter.add(row.toString().length() - rowData.toString().length());
            }
        } catch (Exception e) {
            // 写入产生的脏数据已经由hdfsOutputFormat处理了，这里不用再处理了，只打印日志
            if (numWriteCounter.getLocalValue() % 1000 == 0) {
                LOG.warn("写入hdfs异常:", e);
            }
        }
    }

    @Override
    public void closeInternal() throws IOException {
        closeOutputFormats();
    }

    private Row setChannelInformation(Map<String, Object> event, Object channel, List<String> columns) {
        Row rowData = new Row(columns.size() + 1);
        for (int i = 0; i < columns.size(); i++) {
            rowData.setField(i, event.get(columns.get(i)));
        }
        rowData.setField(rowData.getArity() - 1, channel);
        return rowData;
    }

    private Pair<BaseHdfsOutputFormat, TableInfo> getHdfsOutputFormat(String tablePath, Map event) throws Exception {
        String partitionValue = partitionFormat.currentTime();
        String partitionPath = String.format(HiveUtil.PARTITION_TEMPLATE, partition, partitionValue);
        String hiveTablePath = tablePath + SP + partitionPath;

        BaseHdfsOutputFormat outputFormat = outputFormats.get(hiveTablePath);
        TableInfo tableInfo = checkCreateTable(tablePath, event);
        if (outputFormat == null) {
            hiveUtil.createPartition(tableInfo, partitionPath);
            String path = tableInfo.getPath() + SP + partitionPath;

            outputFormat = createHdfsOutputFormat(tableInfo, path, hiveTablePath);
            outputFormats.put(hiveTablePath, outputFormat);
        }
        return new Pair<BaseHdfsOutputFormat, TableInfo>(outputFormat, tableInfo);
    }

    private BaseHdfsOutputFormat createHdfsOutputFormat(TableInfo tableInfo, String path, String hiveTablePath) {
        try {
            HdfsOutputFormatBuilder hdfsOutputFormatBuilder = this.getHdfsOutputFormatBuilder();
            hdfsOutputFormatBuilder.setPath(path);
            hdfsOutputFormatBuilder.setColumnNames(tableInfo.getColumns());
            hdfsOutputFormatBuilder.setColumnTypes(tableInfo.getColumnTypes());

            BaseHdfsOutputFormat outputFormat = (BaseHdfsOutputFormat) hdfsOutputFormatBuilder.finish();
            outputFormat.setFormatId(hiveTablePath);
            outputFormat.setDirtyDataManager(dirtyDataManager);
            outputFormat.setErrorLimiter(errorLimiter);
            outputFormat.setRuntimeContext(getRuntimeContext());
            outputFormat.setRestoreState(formatStateMap.get(hiveTablePath));
            outputFormat.configure(parameters);
            outputFormat.open(taskNumber, numTasks);

            return outputFormat;
        } catch (Exception e) {
            LOG.error("构建[HdfsOutputFormat]出错:", e);
            throw new RuntimeException(e);
        }
    }

    private TableInfo checkCreateTable(String tablePath, Map event) {
        TableInfo tableInfo = tableCache.get(tablePath);
        if (tableInfo == null) {
            logger.info("tablePath:{} even:{}", tablePath, event);

            String tableName = tablePath;
            if (autoCreateTable && event != null) {
                tableName = MapUtils.getString(event, "table");
                tableName = distributeTableMapping.getOrDefault(tableName, tableName);
            }
            tableInfo = tableInfos.get(tableName);
            if (tableInfo == null) {
                throw new RuntimeException("tableName:" + tableName + " of the tableInfo is null");
            }
            tableInfo.setTablePath(tablePath);
            hiveUtil.createHiveTableWithTableInfo(tableInfo);
            tableCache.put(tablePath, tableInfo);
        }
        return tableInfo;
    }

    private void closeOutputFormats() {
        Iterator<Map.Entry<String, BaseHdfsOutputFormat>> entryIterator = outputFormats.entrySet().iterator();
        while (entryIterator.hasNext()) {
            try {
                Map.Entry<String, BaseHdfsOutputFormat> entry = entryIterator.next();
                entry.getValue().close();
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }

    private HdfsOutputFormatBuilder getHdfsOutputFormatBuilder() {
        HdfsOutputFormatBuilder builder = new HdfsOutputFormatBuilder(fileType);
        builder.setHadoopConfig(hadoopConfig);
        builder.setDefaultFs(defaultFs);
        builder.setWriteMode(writeMode);
        builder.setCompress(compress);
        builder.setCharSetName(charsetName);
        builder.setDelimiter(delimiter);
        builder.setRowGroupSize(rowGroupSize);
        builder.setMaxFileSize(maxFileSize);
        builder.setRestoreConfig(restoreConfig);
        builder.setInitAccumulatorAndDirty(false);

        return builder;
    }

    static class HiveFormatState implements Serializable {
        private Map<String, FormatState> formatStateMap;

        public HiveFormatState(Map<String, FormatState> formatStateMap) {
            this.formatStateMap = formatStateMap;
        }

        public Map<String, FormatState> getFormatStateMap() {
            return formatStateMap;
        }

        public void setFormatStateMap(Map<String, FormatState> formatStateMap) {
            this.formatStateMap = formatStateMap;
        }
    }
}
