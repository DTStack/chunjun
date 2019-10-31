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
import com.dtstack.flinkx.hdfs.writer.HdfsOutputFormat;
import com.dtstack.flinkx.hdfs.writer.HdfsOutputFormatBuilder;
import com.dtstack.flinkx.hive.TableInfo;
import com.dtstack.flinkx.hive.TimePartitionFormat;
import com.dtstack.flinkx.hive.util.DBUtil;
import com.dtstack.flinkx.hive.util.HiveUtil;
import com.dtstack.flinkx.hive.util.PathConverterUtil;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author toutian
 */
public class HiveOutputFormat extends RichOutputFormat {

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

    protected String defaultFS;

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
    private Map<String, HdfsOutputFormat> outputFormats;

    @Override
    public void configure(org.apache.flink.configuration.Configuration parameters) {
        this.parameters = parameters;

        partitionFormat = TimePartitionFormat.getInstance(partitionType);
        tableCache = new HashMap<String, TableInfo>();
        outputFormats = new HashMap<String, HdfsOutputFormat>();
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;

        DBUtil.ConnectionInfo connectionInfo = new DBUtil.ConnectionInfo();
        connectionInfo.setJdbcUrl(jdbcUrl);
        connectionInfo.setUsername(username);
        connectionInfo.setPassword(password);
        connectionInfo.setHiveConf(hadoopConfig);
        connectionInfo.setJobId(jobId);
        connectionInfo.setPlugin("writer");

        hiveUtil = new HiveUtil(connectionInfo, writeMode);
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

        flushOutputFormat();

        super.getFormatState();
        return formatState;
    }

    private void flushOutputFormat() {
        Iterator<Map.Entry<String, HdfsOutputFormat>> entryIterator = outputFormats.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, HdfsOutputFormat> entry = entryIterator.next();
            entry.getValue().getFormatState();
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
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
    }

    @Override
    public void writeRecord(Row row) throws IOException {
        try {
            if (row.getArity() == 2) {
                Object obj = row.getField(0);
                if (obj != null && obj instanceof Map) {
                    emitWithMap((Map<String, Object>) obj, row);
                }
            } else {
                emitWithRow(row);
            }
        } catch (Throwable e) {
            logger.error("{}", e);
        }
    }

    @Override
    public void closeInternal() throws IOException {
        closeOutputFormats();
    }

    private void emitWithMap(Map<String, Object> event, Row row) throws Exception {
        String tablePath = PathConverterUtil.regaxByRules(event, tableBasePath, distributeTableMapping);
        Pair<HdfsOutputFormat, TableInfo> formatPair = getHdfsOutputFormat(tablePath, event);
        Row rowData = setChannelInformation(event, row.getField(1), formatPair.getSecond().getColumns());
        formatPair.getFirst().writeRecord(rowData);
        //row包含map嵌套的数据内容和channel， 而rowData是非常简单的纯数据，此处补上数据差额
        if(bytesWriteCounter != null){
            bytesWriteCounter.add(row.toString().length() - rowData.toString().length());
        }
    }

    private Row setChannelInformation(Map<String, Object> event, Object channel, List<String> columns) {
        Row rowData = new Row(columns.size() + 1);
        for (int i = 0; i < columns.size(); i++) {
            rowData.setField(i, event.get(columns.get(i)));
        }
        rowData.setField(rowData.getArity() - 1, channel);
        return rowData;
    }

    private void emitWithRow(Row rowData) throws Exception {
        Pair<HdfsOutputFormat, TableInfo> formatPair = getHdfsOutputFormat(tableBasePath, null);
        formatPair.getFirst().writeRecord(rowData);
    }

    private Pair<HdfsOutputFormat, TableInfo> getHdfsOutputFormat(String tablePath, Map event) throws Exception {
        String partitionValue = partitionFormat.currentTime();
        String partitionPath = String.format(HiveUtil.PARTITION_TEMPLATE, partition, partitionValue);
        String hiveTablePath = tablePath + SP + partitionPath;

        HdfsOutputFormat outputFormat = outputFormats.get(hiveTablePath);
        TableInfo tableInfo = checkCreateTable(tablePath, event);
        if (outputFormat == null) {
            hiveUtil.createPartition(tableInfo, partitionPath);
            String path = tableInfo.getPath() + SP + partitionPath;

            HdfsOutputFormatBuilder hdfsOutputFormatBuilder = this.getHdfsOutputFormatBuilder();
            hdfsOutputFormatBuilder.setPath(path);
            hdfsOutputFormatBuilder.setColumnNames(tableInfo.getColumns());
            hdfsOutputFormatBuilder.setColumnTypes(tableInfo.getColumnTypes());

            outputFormat = (HdfsOutputFormat) hdfsOutputFormatBuilder.finish();
            outputFormat.setDirtyDataManager(dirtyDataManager);
            outputFormat.setErrorLimiter(errorLimiter);
            outputFormat.setRuntimeContext(getRuntimeContext());
            outputFormat.configure(parameters);
            outputFormat.open(taskNumber, numTasks);
            outputFormats.put(hiveTablePath, outputFormat);
        }
        return new Pair<HdfsOutputFormat, TableInfo>(outputFormat, tableInfo);
    }

    private TableInfo checkCreateTable(String tablePath, Map event) throws Exception {
        try {
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
        } catch (Throwable e) {
            throw new Exception(e);
        }

    }

    private void closeOutputFormats() {
        Iterator<Map.Entry<String, HdfsOutputFormat>> entryIterator = outputFormats.entrySet().iterator();
        while (entryIterator.hasNext()) {
            try {
                Map.Entry<String, HdfsOutputFormat> entry = entryIterator.next();
                entry.getValue().close();
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }

    private HdfsOutputFormatBuilder getHdfsOutputFormatBuilder() {
        HdfsOutputFormatBuilder builder = new HdfsOutputFormatBuilder(fileType);
        builder.setHadoopConfig(hadoopConfig);
        builder.setDefaultFS(defaultFS);
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

}
