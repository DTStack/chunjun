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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.assembly.CmdLineParams;
import com.dtstack.jlogstash.format.StoreEnum;
import com.dtstack.jlogstash.format.TableInfo;
import com.dtstack.jlogstash.format.dirty.DirtyDataManager;
import com.dtstack.jlogstash.format.plugin.HiveOrcOutputFormat;
import com.dtstack.jlogstash.format.plugin.HiveTextOutputFormat;
import com.dtstack.jlogstash.format.util.HiveUtil;
import com.dtstack.jlogstash.format.util.PathConverterUtil;
import com.google.common.collect.Maps;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.charset.Charset;
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
public class HiveOutputFormat extends BaseOutput {

    private static final long serialVersionUID = -6012196822223887479L;

    private static Logger logger = LoggerFactory.getLogger(Hive.class);

    private static String hadoopConf = System.getenv("HADOOP_CONF_DIR");

    private static String hadoopUserName;

    private Configuration configuration;

    private static Map<String, Object> hadoopConfigMap;

    private static String path;

    private boolean autoCreateTable = false;

    private static String store = "ORC";

    private static String writeMode = "APPEND";

    private static String compression = "NONE";

    private static String charsetName = "UTF-8";

    private Charset charset;

    private static String delimiter = "\u0001";

    @Required(required = true)
    private static String jdbcUrl;

    private String database;

    @Required(required = true)
    private static String username;

    @Required(required = true)
    private static String password;

    @Required(required = true)
    private static String analyticalRules;

    /**
     * 间隔 interval 时间对 outputFormat 进行一次 close，触发输出文件的合并
     */
    public static int interval = 60 * 60 * 1000;

    private long lastTime = System.currentTimeMillis();

    /**
     * 字节数量超过 bufferSize 时，outputFormat 进行一次 close，触发输出文件的合并
     */
    public static int bufferSize = 128 * 1024 * 1024;

    private AtomicLong dataSize = new AtomicLong(0L);

    private ScheduledExecutorService executor;

    private Map<String, TableInfo> tableInfos;

    private Map<String, String> distributeTableMapping;

    @Required(required = true)
    private static String tablesColumn;

    private static String distributeTable;

    private final static String PARTITION_TEMPLATE = "%s=%s";

    private static String partition = "pt";

    private static String partitionType;

    private Map<String, HiveOutputFormat> hdfsOutputFormats = Maps.newConcurrentMap();

    private static Map<String, TableInfo> tableCache = new HashMap<>();

    private Lock lock = new ReentrantLock();

    private HiveUtil hiveUtil;

    private TimePartitionFormat partitionFormat;

    private DirtyDataManager dirtyDataManager;

    private AtomicBoolean dirtyDataRunning = new AtomicBoolean(false);

    static {
        Thread.currentThread().setContextClassLoader(null);
    }

    public HiveOutputFormat(Map config) {
        super(config);
        hiveUtil = new HiveUtil(jdbcUrl, username, password, writeMode);
    }

    @Override
    public void prepare() {
        try {
            charset = Charset.forName(charsetName);
            if (StringUtils.isNotBlank(partitionType) && StringUtils.isNotBlank(partition)) {
                partitionFormat = TimePartitionFormat.getInstance(partitionType);
            }
            formatSchema();
//            setHadoopConfiguration();
            process();
            if (Thread.currentThread().getContextClassLoader() == null) {
                Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
            }
        } catch (Exception e) {
            logger.error("", e);
            System.exit(-1);
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

    @Override
    protected void emit(Map event) {

        String tablePath = PathConverterUtil.regaxByRules(event, path, distributeTableMapping);
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

    public HiveOutputFormat getHdfsOutputFormat(String tablePath, Map event) throws Exception {
        String hiveTablePath = tablePath;
        String partitionPath = "";
        if (partitionFormat != null) {
            partitionPath = String.format(PARTITION_TEMPLATE, partition, partitionFormat.currentTime());
            hiveTablePath += "/" + partitionPath;
        }
        HiveOutputFormat hdfsOutputFormat = hdfsOutputFormats.get(hiveTablePath);
        if (hdfsOutputFormat == null) {
            TableInfo tableInfo = checkCreateTable(tablePath, event);
            hiveUtil.createPartition(tableInfo, partitionPath);
            String path = tableInfo.getPath() + "/" + partitionPath;
            if (StoreEnum.TEXT.name().equalsIgnoreCase(tableInfo.getStore())) {
                hdfsOutputFormat = new HiveTextOutputFormat(configuration, path, tableInfo.getColumns(), tableInfo.getColumnTypes(),
                        compression, writeMode, charset, tableInfo.getDelimiter());
            } else if (StoreEnum.ORC.name().equalsIgnoreCase(tableInfo.getStore())) {
                hdfsOutputFormat = new HiveOrcOutputFormat(configuration, path, tableInfo.getColumns(), tableInfo.getColumnTypes(),
                        compression, writeMode, charset);
            } else {
                throw new UnsupportedOperationException("The hdfs store type is unsupported, please use (" + StoreEnum.listStore() + ")");
            }
            hdfsOutputFormat.configure();
            hdfsOutputFormats.put(hiveTablePath, hdfsOutputFormat);
        }
        return hdfsOutputFormat;
    }

    private TableInfo checkCreateTable(String tablePath, Map event) throws Exception {
        try {
            if (!tableCache.containsKey(tablePath)) {
                synchronized (Hive.class) {
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

    @Override
    public synchronized void release() {
        closeHdfsOutputFormats();
        closeDirtyDataManagerWriter(false);
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

    private void formatSchema() {
        /**
         * 分表的映射关系
         * distributeTableMapping 的数据结构为<tableName,groupName>
         * tableInfos的数据结构为<groupName,TableInfo>
         */
        distributeTableMapping = new HashMap<String, String>();
        if (StringUtils.isNotBlank(distributeTable)) {
            JSONObject distributeTableJson = JSON.parseObject(distributeTable);
            for (Map.Entry<String, Object> entry : distributeTableJson.entrySet()) {
                String groupName = entry.getKey();
                List<String> groupTables = (List<String>) entry.getValue();
                for (String tableName : groupTables) {
                    distributeTableMapping.put(tableName, groupName);
                }
            }
        }

        if (jdbcUrl.contains(";principal=")) {
            String[] jdbcStr = jdbcUrl.split(";principal=");
            jdbcUrl = jdbcStr[0];
        }
        int anythingIdx = StringUtils.indexOf(jdbcUrl, '?');
        if (anythingIdx != -1) {
            database = StringUtils.substring(jdbcUrl, StringUtils.lastIndexOf(jdbcUrl, '/') + 1, anythingIdx);
        } else {
            database = StringUtils.substring(jdbcUrl, StringUtils.lastIndexOf(jdbcUrl, '/') + 1);
        }

        tableInfos = new HashMap<String, TableInfo>();
        JSONObject tableColumnJson = JSON.parseObject(tablesColumn);
        for (Map.Entry<String, Object> entry : tableColumnJson.entrySet()) {
            String tableName = entry.getKey();
            List<Map<String, Object>> tableColumns = (List<Map<String, Object>>) entry.getValue();
            TableInfo tableInfo = new TableInfo(tableColumns.size());
            tableInfo.setDatabase(database);
            tableInfo.addPartition(partition);
            tableInfo.setDelimiter(delimiter);
            tableInfo.setStore(store);
            tableInfo.setTableName(tableName);
            for (Map<String, Object> column : tableColumns) {
                tableInfo.addColumnAndType(MapUtils.getString(column, HiveUtil.TABLE_COLUMN_KEY),  HiveUtil.convertType(MapUtils.getString(column, HiveUtil.TABLE_COLUMN_TYPE)));
            }
            String createTableSql = HiveUtil.getCreateTableHql(tableInfo);
            tableInfo.setCreateTableSql(createTableSql);

            logger.info("TableInfo:{}", tableInfo);

            tableInfos.put(tableName, tableInfo);
        }
        if (StringUtils.isBlank(analyticalRules)) {
            path = tableInfos.entrySet().iterator().next().getValue().getTableName();
        } else {
            path = analyticalRules;
            autoCreateTable = true;
        }
    }

    private void createDirtyData() {
        String taskId = CmdLineParams.getName();
        TableInfo tableInfo = hiveUtil.createDirtyDataTable(taskId);

        this.dirtyDataManager = new DirtyDataManager(tableInfo, configuration);
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

}
