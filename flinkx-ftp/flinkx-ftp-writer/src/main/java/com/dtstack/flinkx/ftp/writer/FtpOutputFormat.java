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
import com.dtstack.flinkx.ftp.FtpConfigConstants;
import com.dtstack.flinkx.ftp.IFtpHandler;
import com.dtstack.flinkx.ftp.SFtpHandler;
import com.dtstack.flinkx.ftp.FtpHandler;
import com.dtstack.flinkx.outputformat.FileOutputFormat;
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.util.SysUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static com.dtstack.flinkx.ftp.FtpConfigConstants.SFTP_PROTOCOL;

/**
 * The OutputFormat Implementation which reads data from ftp servers.
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class FtpOutputFormat extends FileOutputFormat {
    /** 换行符 */
    private static final int NEWLINE = 10;

    /** ftp主机名或ip */
    protected String host;

    /** ftp端口 */
    protected Integer port;

    protected String username;

    protected String password;

    protected String delimiter = ",";

    protected String protocol;

    protected Integer timeout;

    protected String connectMode = FtpConfigConstants.DEFAULT_FTP_CONNECT_PATTERN;

    protected List<String> columnTypes;

    protected List<String> columnNames;

    private transient IFtpHandler ftpHandler;

    private transient OutputStream os;

    private static final String DOT = ".";

    private static final String FILE_SUFFIX = ".csv";

    private static final String OVERWRITE_MODE = "overwrite";

    @Override
    protected void openSource() throws IOException {
        if(SFTP_PROTOCOL.equalsIgnoreCase(protocol)) {
            ftpHandler = new SFtpHandler();
        } else {
            ftpHandler = new FtpHandler();
        }
        ftpHandler.loginFtpServer(host,username,password,port,timeout,connectMode);
    }

    @Override
    protected void checkOutputDir() {
        if(!ftpHandler.isDirExist(outputFilePath)){
            if(!makeDir){
                throw new RuntimeException("Output path not exists:" + outputFilePath);
            }

            ftpHandler.mkDirRecursive(outputFilePath);
        } else {
            if(OVERWRITE_MODE.equalsIgnoreCase(writeMode) && !SP.equals(outputFilePath)){
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
                if (splits.length == 3) {
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

        if (os != null){
            return;
        }

        os = ftpHandler.getOutputStream(tmpPath + SP + currentBlockFileName);
        blockIndex++;
    }

    @Override
    public void moveTemporaryDataBlockFileToDirectory(){
        if (currentBlockFileName == null || !currentBlockFileName.startsWith(DOT)){
            return;
        }

        try{
            String src = path + SP + tmpPath + SP + currentBlockFileName;
            if (!ftpHandler.isFileExist(src)) {
                LOG.warn("block file {} not exists", src);
                return;
            }

            currentBlockFileName = currentBlockFileName.replaceFirst("\\.", StringUtils.EMPTY);
            String dist = path + SP + tmpPath + SP + currentBlockFileName;
            ftpHandler.rename(src, dist);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeSingleRecordToFile(Row row) throws WriteRecordException {
        if(os == null){
            nextBlock();
        }

        String line = StringUtil.row2string(row, columnTypes, delimiter, columnNames);
        try {
            byte[] bytes = line.getBytes(this.charsetName);
            this.os.write(bytes);
            this.os.write(NEWLINE);

            if(restoreConfig.isRestore()){
                lastRow = row;
                rowsOfCurrentBlock++;
            }
        } catch(Exception ex) {
            throw new WriteRecordException(ex.getMessage(), ex);
        }
    }

    @Override
    protected void createFinishedTag() throws IOException {
        LOG.info("Subtask [{}] finished, create dir {}", taskNumber, finishedPath);
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
            readyWrite = ftpHandler.isDirExist(tmpPath + SP + ACTION_FINISHED);
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
            throw new RuntimeException("timeout when gathering finish tags for each subtasks");
        }
    }

    @Override
    protected void coverageData(){
        boolean cleanPath = restoreConfig.isRestore() && OVERWRITE_MODE.equalsIgnoreCase(writeMode) && !SP.equals(path);
        if(cleanPath){
            ftpHandler.deleteAllFilesInDir(path, Arrays.asList(tmpPath));
        }
    }

    @Override
    protected void moveTemporaryDataFileToDirectory(){
        try{
            List<String> files = ftpHandler.getFiles(path + SP + tmpPath);
            for (String file : files) {
                String fileName = file.substring(file.lastIndexOf(SP) + 1);
                if (fileName.endsWith(FILE_SUFFIX) && fileName.startsWith(String.valueOf(taskNumber))){
                    String newPath = path + SP + fileName;
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
            List<String> files = ftpHandler.getFiles(path + SP + tmpPath);
            for (String file : files) {
                String fileName = file.substring(file.lastIndexOf(SP) + 1);
                if (fileName.endsWith(FILE_SUFFIX) && !fileName.startsWith(DOT)){
                    String newPath = path + SP + fileName;
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
        if (os != null){
            os.flush();
            os.close();
            os = null;
        }
    }

    @Override
    protected void clearTemporaryDataFiles() throws IOException {
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
    public float getDeviation() {
        return 1.0F;
    }

    @Override
    protected String getExtension() {
        return ".csv";
    }

}
