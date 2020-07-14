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

package com.dtstack.flinkx.ftp.writer;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.ftp.FtpConfig;
import com.dtstack.flinkx.ftp.FtpHandlerFactory;
import com.dtstack.flinkx.ftp.IFtpHandler;
import com.dtstack.flinkx.outputformat.BaseFileOutputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.util.SysUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * The OutputFormat Implementation which reads data from ftp servers.
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class FtpOutputFormat extends BaseFileOutputFormat {

    protected FtpConfig ftpConfig;

    /** 换行符 */
    private static final int NEWLINE = 10;

    protected List<String> columnTypes;

    protected List<String> columnNames;

    private transient IFtpHandler ftpHandler;

    private static final int FILE_NAME_PART_SIZE = 3;

    private static final String DOT = ".";

    private static final String FILE_SUFFIX = ".csv";

    private static final String OVERWRITE_MODE = "overwrite";
    private transient BufferedWriter writer;

    @Override
    protected void openSource() throws IOException {
        ftpHandler = FtpHandlerFactory.createFtpHandler(ftpConfig.getProtocol());
        ftpHandler.loginFtpServer(ftpConfig);
    }

    @Override
    protected void checkOutputDir() {
        if(!ftpHandler.isDirExist(outputFilePath)){
            if(!makeDir){
                throw new RuntimeException("Output path not exists:" + outputFilePath);
            }

            ftpHandler.mkDirRecursive(outputFilePath);
        } else {
            if(OVERWRITE_MODE.equalsIgnoreCase(ftpConfig.getWriteMode()) && !SP.equals(outputFilePath)){
                ftpHandler.deleteAllFilesInDir(outputFilePath, null);
                ftpHandler.mkDirRecursive(outputFilePath);
            }
        }
    }

    @Override
    protected void cleanDirtyData() {
        int fileIndex = formatState.getFileIndex();
        String lastJobId = formatState.getJobId();
        LOG.info("fileIndex = {}, lastJobId = {}",fileIndex, lastJobId);
        if(org.apache.commons.lang3.StringUtils.isBlank(lastJobId)){
            return;
        }
        List<String> files = ftpHandler.getFiles(outputFilePath);
        files.removeIf(new Predicate<String>() {
            @Override
            public boolean test(String file) {
                String fileName = file.substring(file.lastIndexOf(SP) + 1);
                if(!fileName.contains(lastJobId)){
                    return true;
                }

                String[] splits = fileName.split("\\.");
                if (splits.length == FILE_NAME_PART_SIZE) {
                    return Integer.parseInt(splits[2]) <= fileIndex;
                }

                return true;
            }
        });

        if(CollectionUtils.isNotEmpty(files)){
            for (String file : files) {
                ftpHandler.deleteAllFilesInDir(file, null);
            }
        }
    }

    @Override
    protected void nextBlock(){
        super.nextBlock();

        if (writer != null){
            return;
        }
        String path = tmpPath + SP + currentBlockFileName;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(ftpHandler.getOutputStream(path), ftpConfig.getEncoding()));
        } catch (UnsupportedEncodingException e) {
            LOG.error("exception when create BufferedWriter, path = {}, e = {}", path, ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(e);
        }
        blockIndex++;
    }

    @Override
    public void moveTemporaryDataBlockFileToDirectory(){
        if (currentBlockFileName == null || !currentBlockFileName.startsWith(DOT)){
            return;
        }

        try{
            String src = ftpConfig.getPath() + SP + tmpPath + SP + currentBlockFileName;
            if (!ftpHandler.isFileExist(src)) {
                LOG.warn("block file {} not exists", src);
                return;
            }

            currentBlockFileName = currentBlockFileName.replaceFirst("\\.", StringUtils.EMPTY);
            String dist = ftpConfig.getPath() + SP + tmpPath + SP + currentBlockFileName;
            ftpHandler.rename(src, dist);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeSingleRecordToFile(Row row) throws WriteRecordException {
        try {
            if(writer == null){
                nextBlock();
            }

            String line = StringUtil.row2string(row, columnTypes, ftpConfig.getFieldDelimiter());
            this.writer.write(line);
            this.writer.write(NEWLINE);

            if(restoreConfig.isRestore()){
                lastRow = row;
                rowsOfCurrentBlock++;
            }
        } catch(Exception e) {
            LOG.error("error happened when write single record to file, row = {}, columnTypes = {}, e = {}", row, GsonUtil.GSON.toJson(columnTypes), ExceptionUtil.getErrorMessage(e));
            throw new WriteRecordException(e.getMessage(), e);
        }
    }

    @Override
    protected void createFinishedTag() {
        LOG.info("SubTask [{}] finished, create dir {}", taskNumber, finishedPath);
        String path = outputFilePath + SP + FINISHED_SUBDIR;
        if(taskNumber == 0){
            ftpHandler.mkDirRecursive(path);
        }
        final int maxRetryTime = 15;
        int i = 0;
        try {
            while(!(ftpHandler.isDirExist(path) || i > maxRetryTime)){
                i++;
                TimeUnit.MILLISECONDS.sleep(10);
            }
        }catch (Exception e){
            LOG.error("exception when createFinishedTag, path = {}, e = {}", path, ExceptionUtil.getErrorMessage(e));
        }
        ftpHandler.mkDirRecursive(finishedPath);
    }

    @Override
    protected boolean isTaskEndsNormally(){
        try{
            String state = getTaskState();
            if(!RUNNING_STATE.equals(state)){
                if (!restoreConfig.isRestore()){
                    ftpHandler.deleteAllFilesInDir(tmpPath, null);
                }

                ftpHandler.logoutFtpServer();
                return false;
            }
        } catch (IOException e){
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    protected void createActionFinishedTag() {
        ftpHandler.mkDirRecursive(actionFinishedTag);
    }

    @Override
    protected void waitForActionFinishedBeforeWrite() {
        boolean readyWrite = ftpHandler.isDirExist(actionFinishedTag);
        int n = 0;
        while (!readyWrite){
            if(n > SECOND_WAIT){
                throw new RuntimeException("Wait action finished before write timeout");
            }

            SysUtil.sleep(1000);
            LOG.info("action finished tag path:{}", actionFinishedTag);
            readyWrite = ftpHandler.isDirExist(actionFinishedTag);
            n++;
        }
    }

    @Override
    protected void waitForAllTasksToFinish(){
        final int maxRetryTime = 100;
        int i = 0;
        for (; i < maxRetryTime; i++) {
            int finishedTaskNum = ftpHandler.listDirs(outputFilePath + SP + FINISHED_SUBDIR).size();
            LOG.info("The number of finished task is:{}", finishedTaskNum);
            if(finishedTaskNum == numTasks){
                break;
            }

            SysUtil.sleep(3000);
        }

        if (i == maxRetryTime) {
            ftpHandler.deleteAllFilesInDir(finishedPath, null);
            throw new RuntimeException("timeout when gathering finish tags for each subTasks");
        }
    }

    @Override
    protected void coverageData(){
        boolean cleanPath = restoreConfig.isRestore() && OVERWRITE_MODE.equalsIgnoreCase(ftpConfig.getWriteMode()) && !SP.equals(ftpConfig.getPath());
        if(cleanPath){
            ftpHandler.deleteAllFilesInDir(ftpConfig.getPath(), Collections.singletonList(tmpPath));
        }
    }

    @Override
    protected void moveTemporaryDataFileToDirectory(){
        try{
            List<String> files = ftpHandler.getFiles(tmpPath);
            for (String file : files) {
                String fileName = file.substring(file.lastIndexOf(SP) + 1);
                if (fileName.endsWith(FILE_SUFFIX) && fileName.startsWith(String.valueOf(taskNumber))){
                    String newPath = ftpConfig.getPath() + SP + fileName;
                    LOG.info("Move file {} to path {}", file, newPath);
                    ftpHandler.rename(file, newPath);
                }
            }
        }catch (Exception e){
            throw new RuntimeException("Rename temp file error:", e);
        }
    }

    @Override
    protected void moveAllTemporaryDataFileToDirectory() throws IOException {
        try{
            List<String> files = ftpHandler.getFiles(tmpPath);
            for (String file : files) {
                String fileName = file.substring(file.lastIndexOf(SP) + 1);
                if (fileName.endsWith(FILE_SUFFIX) && !fileName.startsWith(DOT)){
                    String newPath = ftpConfig.getPath() + SP + fileName;
                    LOG.info("Move file {} to path {}", file, newPath);
                    ftpHandler.rename(file, newPath);
                }
            }
        }catch (Exception e){
            throw new RuntimeException("Rename temp file error:", e);
        }
    }

    @Override
    protected void closeSource() throws IOException {
        if (writer != null){
            writer.flush();
            writer.close();
            writer = null;
        }
    }

    @Override
    protected void clearTemporaryDataFiles() {
        ftpHandler.deleteAllFilesInDir(tmpPath, null);
        LOG.info("Delete .data dir:{}", tmpPath);

        ftpHandler.deleteAllFilesInDir(outputFilePath + SP + FINISHED_SUBDIR, null);
        LOG.info("Delete .finished dir:{}", outputFilePath + SP + FINISHED_SUBDIR);
    }

    @Override
    public void flushDataInternal() throws IOException {
        closeSource();
    }

    @Override
    public void closeInternal() throws IOException {
        closeSource();
        super.closeInternal();
    }

    @Override
    public float getDeviation() {
        return 1.0F;
    }

    @Override
    protected String getExtension() {
        return ".csv";
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        notSupportBatchWrite("FtpWriter");
    }
}
