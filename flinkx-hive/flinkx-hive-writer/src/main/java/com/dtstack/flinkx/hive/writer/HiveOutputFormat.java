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
public abstract class HiveOutputFormat extends RichOutputFormat {

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

    /** hdfs高可用配置 */
    protected Map<String,String> hadoopConfig;

//    /** 写入模式 */
//    protected String writeMode;
//
//    /** 压缩方式 */
//    protected String compress;

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

//    protected String charsetName = "UTF-8";

    protected int[] colIndices;

    protected Configuration conf;

    protected int blockIndex = 0;

    protected boolean readyCheckpoint;

    protected Row lastRow;

    private String currentBlockFileNamePrefix;

    protected String currentBlockFileName;

    protected long rowsOfCurrentBlock;

//    protected  long maxFileSize;

    private long lastWriteSize;

    private long nextNumForCheckDataSize = DEFAULT_RECORD_NUM_FOR_CHECK;

    /* ----------以上hdfs原有----------- */

//    protected Map<String, String> hadoopConfigMap;
//    protected Configuration configuration;
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

    private transient AtomicLong dataSize = new AtomicLong(0L);

    private transient ScheduledExecutorService executor;

    private Map<String, HiveOutputFormat> outputFormats = Maps.newConcurrentMap();

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
                format.writeSingleRecordInternal(Row.of(event));
//                format.writeRecord(format.convert2Record(event));
                dataSize.addAndGet(ObjectSizeCalculator.getObjectSize(event));
            } catch (Throwable e) {
                if (dirtyDataRunning.compareAndSet(false, true)) {
                    createDirtyData();
                }
//                dirtyDataManager.writeData(event, e);
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

    public HiveOutputFormat getHdfsOutputFormat(String tablePath, Map event) throws Exception {
        String hiveTablePath = tablePath;
        String partitionPath = "";
        if (partitionFormat != null) {
            partitionPath = String.format(HiveUtil.PARTITION_TEMPLATE, partition, partitionFormat.currentTime());
            hiveTablePath += "/" + partitionPath;
        }
        HiveOutputFormat outputFormat = outputFormats.get(hiveTablePath);
        if (outputFormat == null) {
            TableInfo tableInfo = checkCreateTable(tablePath, event);
            hiveUtil.createPartition(tableInfo, partitionPath);
            String path = tableInfo.getPath() + "/" + partitionPath;

            HiveOutputFormatBuilder builder = new HiveOutputFormatBuilder(tableInfo.getStore());
            builder.setPath(path);
            outputFormat = (HiveOutputFormat) builder.finish();
            outputFormat.configInternal();
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
        Iterator<Map.Entry<String, HiveOutputFormat>> entryIterator = outputFormats.entrySet().iterator();
        while (entryIterator.hasNext()) {
            try {
                Map.Entry<String, HiveOutputFormat> entry = entryIterator.next();
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
//        TableInfo tableInfo = hiveUtil.createDirtyDataTable(taskId);
//
//        this.dirtyDataManager = new HiveDirtyDataManager(tableInfo, configuration);
//        this.dirtyDataManager.open();
    }

    private void closeDirtyDataManagerWriter(boolean reopen) {
        if (dirtyDataRunning.get()) {
            this.dirtyDataManager.close();
            if (reopen) {
                this.dirtyDataManager.open();
            }
        }
    }












    protected void initColIndices() {
        if (fullColumnNames == null || fullColumnNames.size() == 0) {
            fullColumnNames = columnNames;
        }

        if (fullColumnTypes == null || fullColumnTypes.size() == 0) {
            fullColumnTypes = columnTypes;
        }

        colIndices = new int[fullColumnNames.size()];
        for(int i = 0; i < fullColumnNames.size(); ++i) {
            int j = 0;
            for(; j < columnNames.size(); ++j) {
                if(fullColumnNames.get(i).equalsIgnoreCase(columnNames.get(j))) {
                    colIndices[i] = j;
                    break;
                }
            }
            if(j == columnNames.size()) {
                colIndices[i] = -1;
            }
        }
    }

    protected void configInternal() {

    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        if(StringUtils.isNotBlank(fileName)) {
            this.outputFilePath = path + SP + fileName;
        } else {
            this.outputFilePath = path;
        }

        initColIndices();

        conf = HdfsUtil.getHadoopConfig(hadoopConfig, defaultFS);
        fs = FileSystem.get(conf);
        Path dir = new Path(outputFilePath);
        // dir不能是文件
        if(fs.exists(dir) && fs.isFile(dir)){
            throw new RuntimeException("Can't write new files under common file: " + dir + "\n"
                    + "One can only write new files under directories");
        }

        configInternal();

        currentBlockFileNamePrefix = taskNumber + "." + jobId;
        tmpPath = outputFilePath + SP + DATA_SUBDIR;
        finishedPath = outputFilePath + SP + FINISHED_SUBDIR + SP + taskNumber;

        LOG.info("Channel:[{}], currentBlockFileNamePrefix:[{}], tmpPath:[{}], finishedPath:[{}]",
                taskNumber, currentBlockFileNamePrefix, tmpPath, finishedPath);

        beforeOpenInternal();
        open();
    }

    @Override
    protected void beforeOpenInternal(){
        if(taskNumber > 0){
            return;
        }

        if (formatState == null || formatState.getState() == null) {
            try {
                LOG.info("Delete [.data] dir before write records");
                clearTemporaryDataFiles();
            } catch (Exception e) {
                LOG.warn("Clean temp dir error before write records:{}", e.getMessage());
            }
        } else {
            alignHistoryFiles();
        }
    }

    private void alignHistoryFiles(){
        try{
            PathFilter pathFilter = path -> !path.getName().startsWith(".");
            FileStatus[] files = fs.listStatus(new Path(tmpPath), pathFilter);
            if(files == null || files.length == 0){
                return;
            }

            List<String> fileNames = Lists.newArrayList();
            for (FileStatus file : files) {
                fileNames.add(file.getPath().getName());
            }

            List<String> deleteFiles = Lists.newArrayList();
            for (String fileName : fileNames) {
                String targetName = fileName.substring(fileName.indexOf(".")+1);
                int num = 0;
                for (String name : fileNames) {
                    if(targetName.equals(name.substring(name.indexOf(".")+1))){
                        num++;
                    }
                }

                if(num < numTasks){
                    deleteFiles.add(fileName);
                }
            }

            for (String fileName : deleteFiles) {
                fs.delete(new Path(tmpPath + SP + fileName), true);
            }
        } catch (Exception e){
            throw new RuntimeException("align files error:", e);
        }
    }

    @Override
    public FormatState getFormatState() {
        if (!restoreConfig.isRestore() || lastRow == null){
            return null;
        }

        try{
            boolean overMaxRows = rowsOfCurrentBlock > restoreConfig.getMaxRowNumForCheckpoint();
            if (readyCheckpoint || overMaxRows){
                flushBlock();

                numWriteCounter.add(rowsOfCurrentBlock);
                formatState.setState(lastRow.getField(restoreConfig.getRestoreColumnIndex()));
                formatState.setNumberWrite(numWriteCounter.getLocalValue());

                rowsOfCurrentBlock = 0;
                return formatState;
            }

            return null;
        }catch (Exception e){
            try{
                fs.delete(new Path(tmpPath + SP + currentBlockFileName), true);
                fs.close();
                LOG.info("getFormatState:delete block file:[{}]", tmpPath + SP + currentBlockFileName);
            }catch (Exception e1){
                throw new RuntimeException("Delete tmp file:" + currentBlockFileName + " failed when get next block error", e1);
            }

            throw new RuntimeException("Get next block error:", e);
        }
    }

    protected void moveTemporaryDataBlockFileToDirectory(){
        try {
            if (currentBlockFileName != null && currentBlockFileName.startsWith(".")){
                Path src = new Path(tmpPath + SP + currentBlockFileName);

                String dataFileName = currentBlockFileName.replaceFirst("\\.","");
                Path dist = new Path(tmpPath + SP + dataFileName);

                fs.rename(src, dist);
                LOG.info("Rename temporary data block file:{} to:{}", src, dist);
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    protected abstract void open() throws IOException;

    protected void nextBlock(){
        if (restoreConfig.isRestore()){
            moveTemporaryDataBlockFileToDirectory();
            currentBlockFileName = "." + currentBlockFileNamePrefix + "." + blockIndex + getExtension();
        } else {
            currentBlockFileName = currentBlockFileNamePrefix + "." + blockIndex + getExtension();
        }

        nextBlockInternal();
    }

    /**
     * Open next file
     */
    protected abstract void nextBlockInternal();

    /**
     * Get file extension
     */
    protected abstract String getExtension();

    /**
     * Close current file stream
     */
    protected abstract void flushBlock() throws IOException;

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        // CAN NOT HAPPEN
    }

    @Override
    public void tryCleanupOnError() throws Exception {
        if(!restoreConfig.isRestore() && fs != null) {
            LOG.info("Clean temporary data in method tryCleanupOnError");
            clearTemporaryDataFiles();
        }
    }

    private void clearTemporaryDataFiles() throws IOException{
        Path finishedDir = new Path(outputFilePath + SP + FINISHED_SUBDIR);
        fs.delete(finishedDir, true);
        LOG.info("Delete .finished dir:{}", finishedDir);

        Path tmpDir = new Path(outputFilePath + SP + DATA_SUBDIR);
        fs.delete(tmpDir, true);
        LOG.info("Delete .data dir:{}", tmpDir);
    }

    @Override
    protected void afterCloseInternal()  {
        try {
            if(!isTaskEndsNormally()){
                return;
            }

            fs.createNewFile(new Path(finishedPath));
            LOG.info("Create finished tag dir:{}", finishedPath);

            numWriteCounter.add(rowsOfCurrentBlock);

            if(taskNumber == 0) {
                waitForAllTasksToFinish();
                coverageData();
                moveTemporaryDataFileToDirectory();

                LOG.info("The task ran successfully,clear temporary data files");
                clearTemporaryDataFiles();
            }

            fs.close();
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected boolean isTaskEndsNormally() throws IOException{
        String state = getTaskState();
        LOG.info("State of current task is:[{}]", state);
        if(!RUNNING_STATE.equals(state)){
            if (!restoreConfig.isRestore()){
                LOG.info("The task does not end normally, clear the temporary data file");
                clearTemporaryDataFiles();
            }
            fs.close();
            return false;
        }

        return true;
    }

    private void waitForAllTasksToFinish() throws IOException{
        Path finishedDir = new Path(outputFilePath + SP + FINISHED_SUBDIR);
        final int maxRetryTime = 100;
        int i = 0;
        for(; i < maxRetryTime; ++i) {
            if(fs.listStatus(finishedDir).length == numTasks) {
                break;
            }
            SysUtil.sleep(3000);
        }

        if (i == maxRetryTime) {
            String subTaskDataPath = outputFilePath + SP + DATA_SUBDIR;
            fs.delete(new Path(subTaskDataPath), true);
            LOG.info("waitForAllTasksToFinish: delete path:[{}]", subTaskDataPath);

            fs.delete(finishedDir, true);
            LOG.info("waitForAllTasksToFinish: delete finished dir:[{}]", finishedDir);

            throw new RuntimeException("timeout when gathering finish tags for each subtasks");
        }
    }

    private void coverageData() throws IOException{
        if(APPEND_MODE.equalsIgnoreCase(writeMode)){
            return;
        }

        LOG.info("Overwrite the original data");
        PathFilter pathFilter = path -> !path.getName().startsWith(".");

        Path dir = new Path(outputFilePath);
        if(fs.exists(dir)) {
            FileStatus[] dataFiles = fs.listStatus(dir, pathFilter);
            for(FileStatus dataFile : dataFiles) {
                LOG.info("coverageData:delete path:[{}]", dataFile.getPath());
                fs.delete(dataFile.getPath(), true);
            }

            LOG.info("coverageData:make dir:[{}]", outputFilePath);
            fs.mkdirs(dir);
        }
    }

    private void moveTemporaryDataFileToDirectory() throws IOException{
        PathFilter pathFilter = path -> !path.getName().startsWith(".");
        Path dir = new Path(outputFilePath);
        List<FileStatus> dataFiles = new ArrayList<>();
        Path tmpDir = new Path(outputFilePath + SP + DATA_SUBDIR);

        FileStatus[] historyTmpDataDir = fs.listStatus(tmpDir);
        for (FileStatus fileStatus : historyTmpDataDir) {
            if (fileStatus.isFile()){
                dataFiles.addAll(Arrays.asList(fs.listStatus(fileStatus.getPath(), pathFilter)));
            }
        }

        for(FileStatus dataFile : dataFiles) {
            fs.rename(dataFile.getPath(), dir);
            LOG.info("Rename temp file:{} to dir:{}", dataFile.getPath(), dir);
        }
    }

    /**
     * Get the deviation
     */
    protected abstract float getDeviation();

    protected void checkWriteSize() {
        if(numWriteCounter.getLocalValue() < nextNumForCheckDataSize){
            return;
        }

        if (getCurrentFileSize() > maxFileSize){
            try{
                flushBlock();
                lastWriteSize = bytesWriteCounter.getLocalValue();
            }catch (IOException e){
                throw new RuntimeException(e);
            }

            nextBlock();
        }

        nextNumForCheckDataSize = getNextNumForCheckDataSize();
        LOG.info("The next record number for check data size is:[{}],current write number is:[{}]", nextNumForCheckDataSize, numWriteCounter.getLocalValue());
    }

    private long getCurrentFileSize(){
        long currentFileSize = (long)(getDeviation() * (bytesWriteCounter.getLocalValue() - lastWriteSize));
        LOG.info("Current file size:[{}]", currentFileSize);

        return  currentFileSize;
    }

    private long getNextNumForCheckDataSize(){
        long totalBytesWrite = bytesWriteCounter.getLocalValue();
        long totalRecordWrite = numWriteCounter.getLocalValue();

        float eachRecordSize = (totalBytesWrite * getDeviation()) / totalRecordWrite;

        long currentFileSize = getCurrentFileSize();
        long recordNum = (long)((maxFileSize - currentFileSize) / eachRecordSize);

        return totalRecordWrite + recordNum;
    }

    @Override
    protected boolean needWaitAfterCloseInternal() {
        return true;
    }

}
