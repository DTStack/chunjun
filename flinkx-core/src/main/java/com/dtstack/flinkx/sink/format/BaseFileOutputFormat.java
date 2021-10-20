/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.sink.format;

import com.dtstack.flinkx.conf.BaseFileConf;
import com.dtstack.flinkx.enums.Semantic;
import com.dtstack.flinkx.enums.SizeUnitType;
import com.dtstack.flinkx.sink.WriteMode;
import com.dtstack.flinkx.throwable.WriteRecordException;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jiangbo
 * @date 2019/8/28
 */
public abstract class BaseFileOutputFormat extends BaseRichOutputFormat {

    protected static final String TMP_DIR_NAME = ".data";
    protected BaseFileConf baseFileConf;
    /** The first half of the file name currently written */
    protected String currentFileNamePrefix;
    /** Full file name */
    protected String currentFileName;
    /** Data file write path */
    protected String outputFilePath;
    /** Temporary data file write path, outputFilePath + /.data */
    protected String tmpPath;

    protected long sumRowsOfBlock;
    protected long rowsOfCurrentBlock;
    /** Current file index number */
    protected int currentFileIndex = 0;

    protected List<String> preCommitFilePathList = new ArrayList<>();
    protected long nextNumForCheckDataSize;
    protected long lastWriteTime = System.currentTimeMillis();

    @Override
    public void initializeGlobal(int parallelism) {
        initVariableFields();
        if (WriteMode.OVERWRITE.name().equalsIgnoreCase(baseFileConf.getWriteMode())
                && StringUtils.isBlank(baseFileConf.getSavePointPath())) {
            // not delete the data directory when restoring from checkpoint
            deleteDataDir();
        } else {
            deleteTmpDataDir();
        }
        checkOutputDir();
    }

    @Override
    public void finalizeGlobal(int parallelism) {
        initVariableFields();
        moveAllTmpDataFileToDir();
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        super.checkpointMode = CheckpointingMode.EXACTLY_ONCE;
        super.semantic = Semantic.EXACTLY_ONCE;
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        if (null != formatState && formatState.getFileIndex() > -1) {
            currentFileIndex = formatState.getFileIndex() + 1;
        }
        LOG.info("Start current File Index:{}", currentFileIndex);

        currentFileNamePrefix = jobId + "_" + taskNumber;
        LOG.info("Channel:[{}], currentFileNamePrefix:[{}]", taskNumber, currentFileNamePrefix);

        initVariableFields();
    }

    protected void initVariableFields() {
        // The file name here is actually the partition name
        if (StringUtils.isNotBlank(baseFileConf.getFileName())) {
            outputFilePath =
                    baseFileConf.getPath() + File.separatorChar + baseFileConf.getFileName();
        } else {
            outputFilePath = baseFileConf.getPath();
        }
        tmpPath = outputFilePath + File.separatorChar + TMP_DIR_NAME;
        nextNumForCheckDataSize = baseFileConf.getNextCheckRows();
        openSource();
    }

    protected void nextBlock() {
        currentFileName = currentFileNamePrefix + "_" + currentFileIndex + getExtension();
    }

    @Override
    protected void writeMultipleRecordsInternal() {
        throw new UnsupportedOperationException("Do not support batch write");
    }

    @Override
    public void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        writeSingleRecordToFile(rowData);
        rowsOfCurrentBlock++;
        checkCurrentFileSize();
        lastRow = rowData;
        lastWriteTime = System.currentTimeMillis();
    }

    private void checkCurrentFileSize() {
        if (numWriteCounter.getLocalValue() < nextNumForCheckDataSize) {
            return;
        }
        long currentFileSize = getCurrentFileSize();
        if (currentFileSize > baseFileConf.getMaxFileSize()) {
            flushData();
        }
        nextNumForCheckDataSize += baseFileConf.getNextCheckRows();
        LOG.info(
                "current file: {}, size = {}, nextNumForCheckDataSize = {}",
                currentFileName,
                SizeUnitType.readableFileSize(currentFileSize),
                nextNumForCheckDataSize);
    }

    public void flushData() {
        if (rowsOfCurrentBlock != 0) {
            flushDataInternal();
            sumRowsOfBlock += rowsOfCurrentBlock;
            LOG.info(
                    "flush file:{}, rowsOfCurrentBlock = {}, sumRowsOfBlock = {}",
                    currentFileName,
                    rowsOfCurrentBlock,
                    sumRowsOfBlock);
            rowsOfCurrentBlock = 0;
        }
    }

    @Override
    protected void preCommit() {
        flushData();
        if (sumRowsOfBlock != 0) {
            preCommitFilePathList = copyTmpDataFileToDir();
        }

        snapshotWriteCounter.add(sumRowsOfBlock);
        sumRowsOfBlock = 0;
        formatState.setJobId(jobId);
        formatState.setFileIndex(currentFileIndex - 1);
    }

    @Override
    public void commit(long checkpointId) {
        deleteDataFiles(preCommitFilePathList, tmpPath);
        preCommitFilePathList.clear();
    }

    @Override
    public void rollback(long checkpointId) {
        deleteDataFiles(preCommitFilePathList, outputFilePath);
        preCommitFilePathList.clear();
    }

    @Override
    public void closeInternal() throws IOException {
        flushData();
        snapshotWriteCounter.add(sumRowsOfBlock);
        sumRowsOfBlock = 0;
        closeSource();
    }

    /** Check whether the writing path exists and whether it is a directory */
    protected abstract void checkOutputDir();

    /** Overwrite mode to clear the data file directory */
    protected abstract void deleteDataDir();

    /** Clear temporary data files */
    protected abstract void deleteTmpDataDir();

    /** Open resource */
    protected abstract void openSource();

    /**
     * Get file suffix
     *
     * @return .gz
     */
    protected abstract String getExtension();

    /**
     * Get the actual size of the file currently written
     *
     * @return
     */
    protected abstract long getCurrentFileSize();

    /**
     * Write single data to file
     *
     * @param rowData Data to be written
     * @throws WriteRecordException Dirty data abnormal
     */
    protected abstract void writeSingleRecordToFile(RowData rowData) throws WriteRecordException;

    /** flush data to storage media */
    protected abstract void flushDataInternal();

    /**
     * copy the temporary data file corresponding to the channel index to the official path
     *
     * @return pre Commit File Path List
     */
    protected abstract List<String> copyTmpDataFileToDir();

    /** Delete the data files submitted in the pre-submission phase under the official directory */
    protected abstract void deleteDataFiles(List<String> preCommitFilePathList, String path);

    /**
     * It is closed normally, triggering files in the .data directory to move to the data directory
     */
    protected abstract void moveAllTmpDataFileToDir();

    /** close Source */
    protected abstract void closeSource();

    /**
     * Get file compression ratio
     *
     * @return 压缩比 < 1
     */
    public abstract float getDeviation();

    public long getLastWriteTime() {
        return lastWriteTime;
    }

    public void setBaseFileConf(BaseFileConf baseFileConf) {
        this.baseFileConf = baseFileConf;
    }
}
