/**
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

import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.hdfs.writer.HdfsOutputFormat;
import com.dtstack.flinkx.hdfs.writer.HdfsOutputFormatBuilder;
import com.dtstack.flinkx.hive.TableInfo;
import com.dtstack.flinkx.hive.TimePartitionFormat;
import com.dtstack.flinkx.hive.dirty.HiveDirtyDataManager;
import com.dtstack.flinkx.hive.util.HdfsUtil;
import com.dtstack.flinkx.hive.util.HiveUtil;
import com.dtstack.flinkx.hive.util.PathConverterUtil;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.SysUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author toutian
 */
public class HiveOutputFormat extends org.apache.flink.api.common.io.RichOutputFormat<Row> {

    private static final long serialVersionUID = -6012196822223887479L;

    private static Logger logger = LoggerFactory.getLogger(HiveOutputFormat.class);

    private static final int DEFAULT_RECORD_NUM_FOR_CHECK = 1000;

    protected int rowGroupSize;

//    protected static final String APPEND_MODE = "APPEND";
//
//    private static final String DATA_SUBDIR = ".data";
//
//    private static final String FINISHED_SUBDIR = ".finished";

    private static final String SP = "/";

    private FileSystem fs;

    private String outputFilePath;

    /**
     * hdfs高可用配置
     */
    protected Map<String, String> hadoopConfig;

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

    protected boolean readyCheckpoint;

    protected Row lastRow;

    protected long rowsOfCurrentBlock;

    protected long maxFileSize;

//    private long lastWriteSize;

//    private long nextNumForCheckDataSize = DEFAULT_RECORD_NUM_FOR_CHECK;

    /* ----------以上hdfs插件参数保持不变----------- */

    protected RestoreConfig restoreConfig;
    protected Map<String, TableInfo> tableInfos;
    protected Map<String, String> distributeTableMapping;
    protected String partition;
    protected String partitionType;
    protected long interval;
    protected long bufferSize;
    protected String jdbcUrl;
    protected String username;
    protected String password;
    protected String tableBasePath;
    protected boolean autoCreateTable;

    private transient HiveUtil hiveUtil;

    private transient ScheduledExecutorService executor;

    private transient TimePartitionFormat partitionFormat;

    private transient HiveDirtyDataManager hiveDirtyDataManager;

    private Lock lock = new ReentrantLock();

    private long lastTime = System.currentTimeMillis();

    private AtomicLong dataSize = new AtomicLong(0L);

    private AtomicBoolean dirtyDataRunning = new AtomicBoolean(false);

    private int taskNumber;
    private int numTasks;

    /**
     * fixme table 注意不要重复创建。需要分布式一致性
     */
    private Map<String, TableInfo> tableCache = new HashMap<>();

    private Map<String, HdfsOutputFormat> outputFormats = Maps.newConcurrentMap();

    @Override
    public void configure(org.apache.flink.configuration.Configuration parameters) {
        hiveUtil = new HiveUtil(jdbcUrl, username, password, writeMode);
        if (StringUtils.isNotBlank(partitionType) && StringUtils.isNotBlank(partition)) {
            partitionFormat = TimePartitionFormat.getInstance(partitionType);
        }

        conf = HdfsUtil.getHadoopConfig(hadoopConfig, defaultFS);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;
    }

    @Override
    public void writeRecord(Row row) throws IOException {
        try {
            lock.lockInterruptibly();
            try {
                if (row.getArity() == 2) {
                    Object obj = row.getField(0);
                    if (obj != null && obj instanceof Map) {
                        emitWithMap((Map<String, Object>) obj, row.getField(1));
                    }
                } else {
                    emitWithRow(row);
                }
            } catch (Throwable e) {
                if (dirtyDataRunning.compareAndSet(false, true)) {
                    createDirtyData();
                }
                hiveDirtyDataManager.writeData(null, e);
                logger.error("", e);
            }
        } catch (Throwable e) {
            logger.error("", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        closeHdfsOutputFormats();
        closeDirtyDataManagerWriter(false);
    }

    private void emitWithMap(Map<String, Object> event, Object channel) throws Exception {
        String tablePath = PathConverterUtil.regaxByRules(event, tableBasePath, distributeTableMapping);
        Pair<HdfsOutputFormat, TableInfo> formatPair = getHdfsOutputFormat(tablePath, event);
        Row rowData = setChannelInformation(event, channel, formatPair.getSecond().getColumns());
        formatPair.getFirst().writeRecord(rowData);

        dataSize.addAndGet(ObjectSizeCalculator.getObjectSize(event));
    }

    private Row setChannelInformation(Map<String, Object> event, Object channel, List<String> columns) {
        Row rowData = new Row(event.size() + 1);
        for (int i = 0; i < columns.size(); i++) {
            rowData.setField(i, event.get(columns.get(i)));
        }
        rowData.setField(rowData.getArity() - 1, channel);
        return rowData;
    }

    private void emitWithRow(Row rowData) throws Exception {
        Pair<HdfsOutputFormat, TableInfo> formatPair = getHdfsOutputFormat(tableBasePath, null);
        formatPair.getFirst().writeRecord(rowData);
        dataSize.addAndGet(ObjectSizeCalculator.getObjectSize(rowData));
    }

    private Pair<HdfsOutputFormat, TableInfo> getHdfsOutputFormat(String tablePath, Map event) throws Exception {
        String hiveTablePath = tablePath;
        String partitionPath = "";
        if (partitionFormat != null) {
            partitionPath = String.format(HiveUtil.PARTITION_TEMPLATE, partition, partitionFormat.currentTime());
            hiveTablePath += "/" + partitionPath;
        }
        HdfsOutputFormat outputFormat = outputFormats.get(hiveTablePath);
        TableInfo tableInfo = checkCreateTable(tablePath, event);
        if (outputFormat == null) {
            hiveUtil.createPartition(tableInfo, partitionPath);
            String path = tableInfo.getPath() + "/" + partitionPath;

            HdfsOutputFormatBuilder hdfsOutputFormatBuilder = this.getHdfsOutputFormatBuilder();
            hdfsOutputFormatBuilder.setPath(path);
            hdfsOutputFormatBuilder.setColumnNames(tableInfo.getColumns());
            hdfsOutputFormatBuilder.setColumnTypes(tableInfo.getColumnTypes());

            outputFormat = (HdfsOutputFormat) hdfsOutputFormatBuilder.finish();
            outputFormat.setRuntimeContext(getRuntimeContext());
            outputFormat.configure(null);
            outputFormat.open(taskNumber, numTasks);
            outputFormats.put(hiveTablePath, outputFormat);
        }
        return new Pair<HdfsOutputFormat, TableInfo>(outputFormat, tableInfo);
    }

    private TableInfo checkCreateTable(String tablePath, Map event) throws Exception {
        try {
            if (!tableCache.containsKey(tablePath)) {
                synchronized (HiveOutputFormat.class) {
                    if (!tableCache.containsKey(tablePath)) {

                        logger.info("tablePath:{} even:{}", tablePath, event);

                        String tableName = tablePath;
                        if (autoCreateTable) {
                            tableName = MapUtils.getString(event, "table");
                            tableName = distributeTableMapping.getOrDefault(tableName, tableName);
                        }
                        TableInfo tableInfo = tableInfos.get(tableName);
                        if (tableInfo == null) {
                            throw new RuntimeException("tableName:" + tableName + " of the tableInfo is null");
                        }
                        tableInfo.setTablePath(tablePath);
                        hiveUtil.createHiveTableWithTableInfo(tableInfo);
                        tableCache.put(tablePath, tableInfo);
                        return tableInfo;
                    }
                }
            }
            return tableCache.get(tablePath);
        } catch (Throwable e) {
            throw new Exception(e);
        }

    }

    private void closeHdfsOutputFormats() {
        Iterator<Map.Entry<String, HdfsOutputFormat>> entryIterator = outputFormats.entrySet().iterator();
        while (entryIterator.hasNext()) {
            try {
                Map.Entry<String, HdfsOutputFormat> entry = entryIterator.next();
                entry.getValue().close();
//                if (entry.getValue().isTimeout(TimePartitionFormat.getPartitionEnum())) {
//                    entryIterator.remove();
//                }
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }

    private void createDirtyData() {
//        String taskId = CmdLineParams.getName();
//        TableInfo tableInfo = hiveUtil.createDirtyDataTable(jobId);
//
        this.hiveDirtyDataManager = new HiveDirtyDataManager(taskNumber + "" + numTasks, tableInfo, conf);
        this.hiveDirtyDataManager.open();
    }

    private void closeDirtyDataManagerWriter(boolean reopen) {
        if (dirtyDataRunning.get()) {
            this.hiveDirtyDataManager.close();
        }
    }

    private HdfsOutputFormatBuilder getHdfsOutputFormatBuilder() {
        HdfsOutputFormatBuilder builder = new HdfsOutputFormatBuilder(fileType);
        builder.setHadoopConfig(hadoopConfig);
        builder.setDefaultFS(defaultFS);
        builder.setWriteMode(writeMode);
        builder.setCompress(compress);
//        builder.setMonitorUrls(monitorUrls);
//        builder.setErrors(errors);
//        builder.setErrorRatio(errorRatio);
//        builder.setFullColumnNames(fullColumnName);
//        builder.setFullColumnTypes(fullColumnType);
//        builder.setDirtyPath(dirtyPath);
//        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
//        builder.setSrcCols(srcCols);
        builder.setCharSetName(charsetName);
        builder.setDelimiter(delimiter);
        builder.setRowGroupSize(rowGroupSize);
        builder.setRestoreConfig(restoreConfig);
        builder.setMaxFileSize(maxFileSize);
        builder.setFlushBlockInterval(interval);

        return builder;
    }

}
