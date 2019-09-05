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


package com.dtstack.flinkx.outputformat;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.writer.FileFlushTimingTrigger;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * @author jiangbo
 * @date 2019/8/28
 */
public abstract class FileOutputFormat extends RichOutputFormat {

    protected Row lastRow;

    protected String currentBlockFileNamePrefix;

    protected String currentBlockFileName;

    protected long sumRowsOfBlock;

    protected long rowsOfCurrentBlock;

    protected  long maxFileSize;

    protected long flushInterval = 0;

    protected static final String APPEND_MODE = "APPEND";

    protected static final String DATA_SUBDIR = ".data";

    protected static final String FINISHED_SUBDIR = ".finished";

    protected static final String SP = "/";

    protected String charsetName = "UTF-8";

    protected String outputFilePath;

    protected String path;

    protected String fileName;

    protected String tmpPath;

    protected String finishedPath;

    /** 写入模式 */
    protected String writeMode;

    /** 压缩方式 */
    protected String compress;

    protected boolean readyCheckpoint;

    protected int blockIndex = 0;

    protected boolean makeDir = true;

    private transient FileFlushTimingTrigger fileSizeChecker;

    private long nextNumForCheckDataSize = 1000;

    private long lastWriteSize;

    protected long lastWriteTime = System.currentTimeMillis();

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        if(restoreConfig.isStream() && flushInterval > 0){
            fileSizeChecker = new FileFlushTimingTrigger(this, flushInterval);
            fileSizeChecker.start();
        }

        initPath();
        openSource();
        actionBeforeWriteData();

        nextBlock();
    }

    private void initPath(){
        if(StringUtils.isNotBlank(fileName)) {
            outputFilePath = path + SP + fileName;
        } else {
            outputFilePath = path;
        }

        currentBlockFileNamePrefix = taskNumber + "." + jobId;
        tmpPath = outputFilePath + SP + DATA_SUBDIR;
        finishedPath = outputFilePath + SP + FINISHED_SUBDIR + SP + taskNumber;

        LOG.info("Channel:[{}], currentBlockFileNamePrefix:[{}], tmpPath:[{}], finishedPath:[{}]",
                taskNumber, currentBlockFileNamePrefix, tmpPath, finishedPath);
    }

    protected void actionBeforeWriteData(){
        if(taskNumber > 0){
            return;
        }

        checkOutputDir();

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

    @Override
    public void writeSingleRecordInternal(Row row) throws WriteRecordException {
        if (restoreConfig.isRestore() && !restoreConfig.isStream()){
            if(lastRow != null){
                readyCheckpoint = !ObjectUtils.equals(lastRow.getField(restoreConfig.getRestoreColumnIndex()),
                        row.getField(restoreConfig.getRestoreColumnIndex()));
            }
        }

        checkSize();

        synchronized (this){
            writeSingleRecordToFile(row);
        }

        lastWriteTime = System.currentTimeMillis();
    }

    private void checkSize() {
        if(numWriteCounter.getLocalValue() < nextNumForCheckDataSize){
            return;
        }

        if(getCurrentFileSize() > maxFileSize){
            try {
                flushData();
                LOG.info("Flush data by check file size");
            } catch (Exception e){
                throw new RuntimeException("Flush data error", e);
            }

            lastWriteSize = bytesWriteCounter.getLocalValue();
        }

        nextNumForCheckDataSize = getNextNumForCheckDataSize();
    }

    private long getCurrentFileSize(){
        return  (long)(getDeviation() * (bytesWriteCounter.getLocalValue() - lastWriteSize));
    }

    private long getNextNumForCheckDataSize(){
        long totalBytesWrite = bytesWriteCounter.getLocalValue();
        long totalRecordWrite = numWriteCounter.getLocalValue();

        float eachRecordSize = (totalBytesWrite * getDeviation()) / totalRecordWrite;

        long currentFileSize = getCurrentFileSize();
        long recordNum = (long)((maxFileSize - currentFileSize) / eachRecordSize);

        return totalRecordWrite + recordNum;
    }

    protected void nextBlock(){
        if (restoreConfig.isRestore()){
            moveTemporaryDataBlockFileToDirectory();
            currentBlockFileName = "." + currentBlockFileNamePrefix + "." + blockIndex + getExtension();
        } else {
            currentBlockFileName = currentBlockFileNamePrefix + "." + blockIndex + getExtension();
        }
    }

    @Override
    public FormatState getFormatState() {
        if (!restoreConfig.isRestore() || lastRow == null){
            return null;
        }

        try{
            boolean overMaxRows = rowsOfCurrentBlock > restoreConfig.getMaxRowNumForCheckpoint();
            if (restoreConfig.isStream() || readyCheckpoint || overMaxRows){

//                flushData();
//                numWriteCounter.add(rowsOfCurrentBlock);
//                formatState.setNumberWrite(numWriteCounter.getLocalValue());

                moveTemporaryDataFileToDirectory();
                snapshotWriteCounter.add(sumRowsOfBlock);
                formatState.setNumberWrite(snapshotWriteCounter.getLocalValue());

                if (!restoreConfig.isStream()){
                    formatState.setState(lastRow.getField(restoreConfig.getRestoreColumnIndex()));
                }

                rowsOfCurrentBlock = 0;
                return formatState;
            }

            return null;
        }catch (Exception e){
            try{
                closeSource();
                LOG.info("getFormatState:delete block file:[{}]", tmpPath + SP + currentBlockFileName);
            }catch (Exception e1){
                throw new RuntimeException("Delete tmp file:" + currentBlockFileName + " failed when get next block error", e1);
            }

            throw new RuntimeException("Get next block error:", e);
        }
    }

    @Override
    public void closeInternal() throws IOException {
        if(fileSizeChecker != null){
            fileSizeChecker.stop();
        }

        readyCheckpoint = false;

        //no checkpoint
        if(!restoreConfig.isRestore() && isTaskEndsNormally()){
            moveTemporaryDataBlockFileToDirectory();
        }

        closeSource();
    }

    @Override
    protected void afterCloseInternal()  {
        try {
            if(!isTaskEndsNormally()){
                return;
            }

            createFinishedTag();

            //fixme 正常的也不能直接提交数据，要根据checkpoint来提交数据，才能保证 writecount 的统计值
//            if(restoreConfig.isRestore()){
//                numWriteCounter.add(rowsOfCurrentBlock);
//            }

            if(taskNumber == 0) {
                waitForAllTasksToFinish();

                if(!APPEND_MODE.equalsIgnoreCase(writeMode)){
                    coverageData();
                }

//                moveTemporaryDataFileToDirectory();
                if(!restoreConfig.isRestore()) {
                    moveTemporaryDataFileToDirectory();
                }

                LOG.info("The task ran successfully,clear temporary data files");
                clearTemporaryDataFiles();
            }

            closeSource();
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

            closeSource();
            return false;
        }

        return true;
    }

    @Override
    public void tryCleanupOnError() throws Exception {
        if(!restoreConfig.isRestore()) {
            LOG.info("Clean temporary data in method tryCleanupOnError");
            clearTemporaryDataFiles();
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        // CAN NOT HAPPEN
    }

    @Override
    protected boolean needWaitAfterCloseInternal() {
        return true;
    }

    public String getPath() {
        return path;
    }

    public void flushData() throws IOException{
        synchronized (this){
            flushDataInternal();
            sumRowsOfBlock += rowsOfCurrentBlock;
            LOG.info("flush file:{} rows:{} sumRowsOfBlock:{}", currentBlockFileName, rowsOfCurrentBlock, sumRowsOfBlock);
        }
    }

    public long getLastWriteTime() {
        return lastWriteTime;
    }

    protected abstract void flushDataInternal() throws IOException;

    protected abstract void writeSingleRecordToFile(Row row) throws WriteRecordException;

    protected abstract void createFinishedTag() throws IOException;

    protected abstract void moveTemporaryDataBlockFileToDirectory();

    protected abstract void waitForAllTasksToFinish() throws IOException;

    protected abstract void coverageData() throws IOException;

    protected abstract void moveTemporaryDataFileToDirectory() throws IOException;

    protected abstract void checkOutputDir();

    protected abstract void openSource() throws IOException;

    protected abstract void closeSource() throws IOException;

    protected abstract void alignHistoryFiles();

    protected abstract void clearTemporaryDataFiles() throws IOException;

    public abstract float getDeviation();

    protected abstract String getExtension();
}
