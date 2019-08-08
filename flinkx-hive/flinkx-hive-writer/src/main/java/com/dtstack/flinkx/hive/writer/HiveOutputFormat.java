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
import com.dtstack.flinkx.hive.EStoreType;
import com.dtstack.flinkx.hive.TableInfo;
import com.dtstack.flinkx.hive.TimePartitionFormat;
import com.dtstack.flinkx.hive.dirty.HiveDirtyDataManager;
import com.dtstack.flinkx.hive.util.HiveUtil;
import com.dtstack.flinkx.hive.util.PathConverterUtil;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.google.common.collect.Maps;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
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
public abstract class HiveOutputFormat extends RichOutputFormat {

    private static final long serialVersionUID = -6012196822223887479L;

    private static Logger logger = LoggerFactory.getLogger(HiveOutputFormat.class);

    private static final String DATA_SUBDIR = ".data";

    private static final String FINISHED_SUBDIR = ".finished";

    private static final String SP = "/";

    protected Map<String, String> hadoopConfigMap;
    protected Map<String, TableInfo> tableInfos;
    protected Map<String, String> distributeTableMapping;
    protected String writeMode;
    protected String compress;
    protected String charsetName;
    protected long maxFileSize;
    protected String partition;
    protected String partitionType;
    protected long interval;
    protected long bufferSize;
    protected String jdbcUrl;
    protected String username;
    protected String password;
    protected String tableBasePath;
    protected boolean autoCreateTable;

    private transient Charset charset;

    private transient HiveUtil hiveUtil;

    private transient long lastTime = System.currentTimeMillis();

    private transient Configuration configuration;

    private transient AtomicLong dataSize = new AtomicLong(0L);

    private transient ScheduledExecutorService executor;

    private Map<String, HiveOutputFormat> hdfsOutputFormats = Maps.newConcurrentMap();

    private static Map<String, TableInfo> tableCache = new HashMap<>();

    private Lock lock = new ReentrantLock();

    private TimePartitionFormat partitionFormat;

    private HiveDirtyDataManager hiveDirtyDataManager;

    private AtomicBoolean dirtyDataRunning = new AtomicBoolean(false);

    @Override
    public void configure(org.apache.flink.configuration.Configuration parameters) {
        try {
            hiveUtil = new HiveUtil(jdbcUrl, username, password, writeMode);
            charset = Charset.forName(charsetName);
            if (StringUtils.isNotBlank(partitionType) && StringUtils.isNotBlank(partition)) {
                partitionFormat = TimePartitionFormat.getInstance(partitionType);
            }
            setHadoopConfiguration();
            process();
        } catch (Exception e) {
            logger.error("", e);
            System.exit(-1);
        }
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        if (row.getArity() == 1){
            Object obj = row.getField(0);
            if(obj instanceof Map) {
                emit((Map<String, Object>) obj);
            }
        }
    }

    private void emit(Map event) {
        String tablePath = PathConverterUtil.regaxByRules(event, tableBasePath, distributeTableMapping);
        try {
            lock.lockInterruptibly();

            HiveOutputFormat format = getHdfsOutputFormat(tablePath, event);

            try {
                format.writeRecord(format.convert2Record(event));
                dataSize.addAndGet(ObjectSizeCalculator.getObjectSize(event));
            } catch (Throwable e) {
                if (dirtyDataRunning.compareAndSet(false, true)) {
                    createDirtyData();
                }
                dirtyDataManager.writeData(event, e);
                logger.error("", e);
            }
        } catch (Throwable e) {
            logger.error("", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {

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

    public HiveOutputFormat getHdfsOutputFormat(String tablePath, Map event) throws Exception {
        String hiveTablePath = tablePath;
        String partitionPath = "";
        if (partitionFormat != null) {
            partitionPath = String.format(HiveUtil.PARTITION_TEMPLATE, partition, partitionFormat.currentTime());
            hiveTablePath += "/" + partitionPath;
        }
        HiveOutputFormat hdfsOutputFormat = hdfsOutputFormats.get(hiveTablePath);
        if (hdfsOutputFormat == null) {
            TableInfo tableInfo = checkCreateTable(tablePath, event);
            hiveUtil.createPartition(tableInfo, partitionPath);
            String path = tableInfo.getPath() + "/" + partitionPath;
            if (EStoreType.TEXT.name().equalsIgnoreCase(tableInfo.getStore())) {
                hdfsOutputFormat = new HiveTextOutputFormat(configuration, path, tableInfo.getColumns(), tableInfo.getColumnTypes(),
                        compress, writeMode, charset, tableInfo.getDelimiter());
            } else if (EStoreType.ORC.name().equalsIgnoreCase(tableInfo.getStore())) {
                hdfsOutputFormat = new HiveOrcOutputFormat(configuration, path, tableInfo.getColumns(), tableInfo.getColumnTypes(),
                        compress, writeMode, charset);
            } else {
                throw new UnsupportedOperationException("The hdfs store type is unsupported, please use (" + StoreEnum.listStore() + ")");
            }
            hdfsOutputFormat.configure();
            hdfsOutputFormats.put(hiveTablePath, hdfsOutputFormat);
        }
        return hdfsOutputFormat;
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
        Iterator<Map.Entry<String, HiveOutputFormat>> entryIterator = hdfsOutputFormats.entrySet().iterator();
        while (entryIterator.hasNext()) {
            try {
                Map.Entry<String, HiveOutputFormat> entry = entryIterator.next();
                entry.getValue().close();
                if (entry.getValue().isTimeout(TimePartitionFormat.getPartitionEnum())) {
                    entryIterator.remove();
                }
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }

    private void createDirtyData() {
        String taskId = CmdLineParams.getName();
        TableInfo tableInfo = hiveUtil.createDirtyDataTable(taskId);

        this.dirtyDataManager = new HiveDirtyDataManager(tableInfo, configuration);
        this.dirtyDataManager.open();
    }

    private void closeDirtyDataManagerWriter(boolean reopen) {
        if (dirtyDataRunning.get()) {
            this.dirtyDataManager.close();
            if (reopen) {
                this.dirtyDataManager.open();
            }
        }
    }


    private void setHadoopConfiguration() throws Exception {
        if (hadoopConfigMap != null) {
            configuration = new Configuration(false);
            for (Map.Entry<String, String> entry : hadoopConfigMap.entrySet()) {
                configuration.set(entry.getKey(), entry.getValue().toString());
            }
            configuration.set("fs.hdfs.impl.disable.cache", "true");
        }
    }

}
