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
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.util.SysUtil;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import static com.dtstack.flinkx.ftp.FtpConfigConstants.SFTP_PROTOCOL;

/**
 * The OutputFormat Implementation which reads data from ftp servers.
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class FtpOutputFormat extends RichOutputFormat {
    /** 换行符 */
    private static final int NEWLINE = 10;

    /** 输出路径 */
    protected String path;

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

    protected String charsetName = "utf-8";

    protected String writeMode = "append";

    protected List<String> columnTypes;

    protected List<String> columnNames;

    private transient IFtpHandler ftpHandler;

    private transient OutputStream os;

    private Row lastRow;

    private long rowsOfCurrentFile;

    private boolean readyCheckpoint;

    private int fileIndex;

    private static final String SP = "/";

    private static String tempPath = ".data";

    private static String finishedTagPath = tempPath + SP + ".finished";

    private static final String DOT = ".";

    private static final String FILE_SUFFIX = ".csv";

    private static final String OVERWRITE_MODE = "overwrite";

    private String currentFileNamePrefix;

    private String currentFileName;

    @Override
    public void configure(Configuration parameters) {
        if(SFTP_PROTOCOL.equalsIgnoreCase(protocol)) {
            ftpHandler = new SFtpHandler();
        } else {
            ftpHandler = new FtpHandler();
        }
        ftpHandler.loginFtpServer(host,username,password,port,timeout,connectMode);
    }

    @Override
    protected boolean needWaitBeforeOpenInternal() {
        return true;
    }

    @Override
    protected void beforeOpenInternal() {
        if(taskNumber == 0) {
            if (restoreConfig.isRestore()){
                if(!ftpHandler.isDirExist(path + SP + finishedTagPath)){
                    ftpHandler.mkDirRecursive(path + SP + finishedTagPath);
                }
            } else if(OVERWRITE_MODE.equalsIgnoreCase(writeMode) && !SP.equals(path)){
                ftpHandler.deleteAllFilesInDir(path, null);
                ftpHandler.mkDirRecursive(path);
            }
        }
    }

    @Override
    public void openInternal(int taskNumber, int numTasks) throws IOException {
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        String dateString = formatter.format(currentTime);
        currentFileNamePrefix = taskNumber + DOT + dateString + DOT + UUID.randomUUID() + FILE_SUFFIX;

        if (!restoreConfig.isRestore()){
            String filePath = path + SP + currentFileNamePrefix;
            this.os = ftpHandler.getOutputStream(filePath);
        }
    }

    private void nextFile() {
        if (os != null){
            return;
        }

        moveTemporaryDataBlockFileToDirectory();

        currentFileName = DOT + fileIndex + currentFileNamePrefix;
        String filePath = path + SP + tempPath + SP + currentFileName;
        os = ftpHandler.getOutputStream(filePath);
        fileIndex++;
    }

    /**
     * rename .xxxxx.csv to xxxxx.csv
     */
    private void moveTemporaryDataBlockFileToDirectory(){
        if (currentFileName == null || !currentFileName.startsWith(DOT)){
            return;
        }

        try{
            String src = path + SP + tempPath + SP + currentFileName;
            currentFileName = currentFileName.replaceFirst("\\.", StringUtils.EMPTY);
            String dist = path + SP + tempPath + SP + currentFileName;
            ftpHandler.rename(src, dist);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private void flushFile() throws IOException{
        if (os != null){
            os.flush();
            os = null;
        }
    }

    @Override
    public FormatState getFormatState() {
        if (!restoreConfig.isRestore() || lastRow == null){
            return null;
        }

        try{
            if (readyCheckpoint || rowsOfCurrentFile > restoreConfig.getMaxRowNumForCheckpoint()){
                flushFile();

                numWriteCounter.add(rowsOfCurrentFile);
                rowsOfCurrentFile = 0;

                formatState.setState(lastRow.getField(restoreConfig.getRestoreColumnIndex()));
                formatState.setNumberWrite(numWriteCounter.getLocalValue());
                return formatState;
            }

            return null;
        }catch (Exception e){
            ftpHandler.deleteAllFilesInDir(path + SP + tempPath + SP + currentFileName, null);
            throw new RuntimeException("Get next file error:", e);
        }
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        if (restoreConfig.isRestore()){
            nextFile();

            if(lastRow != null){
                readyCheckpoint = !ObjectUtils.equals(lastRow.getField(restoreConfig.getRestoreColumnIndex()),
                        row.getField(restoreConfig.getRestoreColumnIndex()));
            }
        }

        String line = StringUtil.row2string(row, columnTypes, delimiter, columnNames);
        try {
            byte[] bytes = line.getBytes(this.charsetName);
            this.os.write(bytes);
            this.os.write(NEWLINE);

            lastRow = row;
            rowsOfCurrentFile++;
        } catch(Exception ex) {
            throw new WriteRecordException(ex.getMessage(), ex);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        // unreachable
    }

    @Override
    protected boolean needWaitAfterCloseInternal(){return restoreConfig.isRestore();}

    @Override
    protected void afterCloseInternal(){
        if(!isTaskEndsNormally()){
            return;
        }

        if (restoreConfig.isRestore() && ftpHandler != null){
            String finishedTag = path + SP + finishedTagPath + SP + taskNumber + "_finished";
            LOG.info("Subtask [{}] finished, create dir {}", taskNumber, finishedTag);
            ftpHandler.mkDirRecursive(finishedTag);
        }

        numWriteCounter.add(rowsOfCurrentFile);

        moveTemporaryDataBlockFileToDirectory();

        if (taskNumber == 0){
            waitForAllTasksToFinish();
            coverageData();
            moveTemporaryDataFileToDirectory();

            ftpHandler.deleteAllFilesInDir(path + SP + tempPath, null);
            ftpHandler.logoutFtpServer();
        }
    }

    private boolean isTaskEndsNormally(){
        try{
            String state = getTaskState();
            if(!RUNNING_STATE.equals(state)){
                if (!restoreConfig.isRestore()){
                    ftpHandler.deleteAllFilesInDir(path + SP + tempPath, null);
                }

                ftpHandler.logoutFtpServer();
                return false;
            }
        } catch (IOException e){
            throw new RuntimeException(e);
        }

        return true;
    }

    private void waitForAllTasksToFinish(){
        final int maxRetryTime = 100;
        int i = 0;
        for (; i < maxRetryTime; i++) {
            int finishedTaskNum = ftpHandler.listDirs(path + SP + finishedTagPath).size();
            LOG.info("The number of finished task is:{}", finishedTaskNum);
            if(finishedTaskNum == numTasks){
                break;
            }

            SysUtil.sleep(3000);
        }

        if (i == maxRetryTime) {
            String finishedTag = path + SP + finishedTagPath + SP + taskNumber + "_finished";
            ftpHandler.deleteAllFilesInDir(finishedTag, null);

            throw new RuntimeException("timeout when gathering finish tags for each subtasks");
        }
    }

    private void coverageData(){
        boolean cleanPath = restoreConfig.isRestore() && OVERWRITE_MODE.equalsIgnoreCase(writeMode) && !SP.equals(path);
        if(cleanPath){
            ftpHandler.deleteAllFilesInDir(path, Arrays.asList(tempPath));
        }
    }

    private void moveTemporaryDataFileToDirectory(){
        try{
            List<String> files = ftpHandler.getFiles(path + SP + tempPath);
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
    public void closeInternal() throws IOException {
        flushFile();
    }

}
