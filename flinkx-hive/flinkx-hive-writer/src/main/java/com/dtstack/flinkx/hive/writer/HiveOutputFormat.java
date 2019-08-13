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
 * @author sishu.yss
 */
public class HiveOutputFormat extends RichOutputFormat {

    private static final long serialVersionUID = -6012196822223887479L;

    private static Logger logger = LoggerFactory.getLogger(HiveOutputFormat.class);

    private static final int DEFAULT_RECORD_NUM_FOR_CHECK = 1000;

    protected int rowGroupSize;

    protected static final String APPEND_MODE = "APPEND";

    private static final String DATA_SUBDIR = ".data";

    private static final String FINISHED_SUBDIR = ".finished";

    private static final String SP = "/";

    private FileSystem fs;

    private String outputFilePath;

    /**
     * hdfs高可用配置
     */
    protected Map<String, String> hadoopConfig;

    protected String store;

    /**
     * 写入模式
     */
    protected String writeMode;

    /**
     * 压缩方式
     */
    protected String compress;

    protected String defaultFS;

    protected String path;

    protected String fileName;

    protected List<String> columnTypes;

    protected List<String> columnNames;

    protected List<String> fullColumnNames;

    protected List<String> fullColumnTypes;

    protected String delimiter;

    protected String tmpPath;

    protected String finishedPath;

    protected String charsetName = "UTF-8";

    protected int[] colIndices;

    protected Configuration conf;

    protected int blockIndex = 0;

    protected boolean readyCheckpoint;

    protected Row lastRow;

    private String currentBlockFileNamePrefix;

    protected String currentBlockFileName;

    protected long rowsOfCurrentBlock;

    protected long maxFileSize;

    private long lastWriteSize;

    private long nextNumForCheckDataSize = DEFAULT_RECORD_NUM_FOR_CHECK;

    /* ----------以上hdfs插件参数保持不变----------- */

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

    private transient long lastTime = System.currentTimeMillis();

    private transient AtomicLong dataSize = new AtomicLong(0L);

    private transient ScheduledExecutorService executor;

    private transient TimePartitionFormat partitionFormat;

    private transient HiveDirtyDataManager hiveDirtyDataManager;

    private Lock lock = new ReentrantLock();

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
        process();
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        if (row.getArity() == 1) {
            Object obj = row.getField(0);
            if (obj instanceof Map) {
                emit((Map<String, Object>) obj);
            }
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
    }

    private void emit(Map event) {
        String tablePath = PathConverterUtil.regaxByRules(event, tableBasePath, distributeTableMapping);
        try {
            lock.lockInterruptibly();

            HdfsOutputFormat format = getHdfsOutputFormat(tablePath, event);

            try {
                format.writeRecord(Row.of(event));
//                format.writeRecord(format.convert2Record(event));
                dataSize.addAndGet(ObjectSizeCalculator.getObjectSize(event));
            } catch (Throwable e) {
                if (dirtyDataRunning.compareAndSet(false, true)) {
                    createDirtyData();
                }
                hiveDirtyDataManager.writeData(event, e);
                logger.error("", e);
            }
        } catch (Throwable e) {
            logger.error("", e);
        } finally {
            lock.unlock();
        }
    }

    public void process() {
        executor = new ScheduledThreadPoolExecutor(1);
        executor.scheduleWithFixedDelay(() -> {
            if ((System.currentTimeMillis() - lastTime >= interval)
                    || dataSize.get() >= bufferSize) {
                try {
                    lock.lockInterruptibly();
                    closeHdfsOutputFormats();
                    closeDirtyDataManagerWriter(true);
                    resetWriteStrategy();
                    logger.warn("hdfs commit again...");
                } catch (InterruptedException e) {
                    logger.error("{}", e);
                } finally {
                    lock.unlock();
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    private void resetWriteStrategy() {
        dataSize.set(0L);
        lastTime = System.currentTimeMillis();
    }

    public HdfsOutputFormat getHdfsOutputFormat(String tablePath, Map event) throws Exception {
        String hiveTablePath = tablePath;
        String partitionPath = "";
        if (partitionFormat != null) {
            partitionPath = String.format(HiveUtil.PARTITION_TEMPLATE, partition, partitionFormat.currentTime());
            hiveTablePath += "/" + partitionPath;
        }
        HdfsOutputFormat outputFormat = outputFormats.get(hiveTablePath);
        if (outputFormat == null) {
            TableInfo tableInfo = checkCreateTable(tablePath, event);
            hiveUtil.createPartition(tableInfo, partitionPath);
            String path = tableInfo.getPath() + "/" + partitionPath;

            HdfsOutputFormatBuilder hdfsOutputFormatBuilder = this.getHdfsOutputFormatBuilder();
            hdfsOutputFormatBuilder.setPath(path);
            hdfsOutputFormatBuilder.setColumnNames(tableInfo.getColumns());
            hdfsOutputFormatBuilder.setColumnTypes(tableInfo.getColumnTypes());

            outputFormat = (HdfsOutputFormat) hdfsOutputFormatBuilder.finish();
            outputFormat.configure(null);
            outputFormat.open(taskNumber, numTasks);
            outputFormats.put(hiveTablePath, outputFormat);


//            if (EStoreType.TEXT.name().equalsIgnoreCase(tableInfo.getStore())) {
//                hdfsOutputFormat = new HiveTextOutputFormat(configuration, path, tableInfo.getColumns(), tableInfo.getColumnTypes(),
//                        compress, writeMode, charset, tableInfo.getDelimiter());
//            } else if (EStoreType.ORC.name().equalsIgnoreCase(tableInfo.getStore())) {
//                hdfsOutputFormat = new HiveOrcOutputFormat(configuration, path, tableInfo.getColumns(), tableInfo.getColumnTypes(),
//                        compress, writeMode, charset);
//            } else {
//                throw new UnsupportedOperationException("The hdfs store type is unsupported, please use (" + StoreEnum.listStore() + ")");
//            }
//            hdfsOutputFormat.configure();
//            outputFormats.put(hiveTablePath, hdfsOutputFormat);
        }
        return outputFormat;
    }

    @Override
    public void close() throws IOException {
        closeHdfsOutputFormats();
        closeDirtyDataManagerWriter(false);
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
        TableInfo tableInfo = hiveUtil.createDirtyDataTable(jobId);

        this.hiveDirtyDataManager = new HiveDirtyDataManager(taskNumber + "" + numTasks, tableInfo, conf);
        this.hiveDirtyDataManager.open();
    }

    private void closeDirtyDataManagerWriter(boolean reopen) {
        if (dirtyDataRunning.get()) {
            this.hiveDirtyDataManager.close();
            if (reopen) {
                this.hiveDirtyDataManager.open();
            }
        }
    }

    private HdfsOutputFormatBuilder getHdfsOutputFormatBuilder() {
        HdfsOutputFormatBuilder builder = new HdfsOutputFormatBuilder(store);
        builder.setHadoopConfig(hadoopConfig);
        builder.setDefaultFS(defaultFS);
        builder.setPath(path);
        builder.setFileName(fileName);
        builder.setWriteMode(writeMode);
//        builder.setColumnNames(columnName);
//        builder.setColumnTypes(columnType);
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

        return builder;
    }

}
