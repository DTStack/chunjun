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
import com.dtstack.flinkx.ftp.FtpHandler;
import com.dtstack.flinkx.ftp.SFtpHandler;
import com.dtstack.flinkx.ftp.StandardFtpHandler;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.util.SysUtil;
import org.apache.commons.lang.ObjectUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
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

    protected Integer timeout = 60000;

    protected String connectMode = FtpConfigConstants.DEFAULT_FTP_CONNECT_PATTERN;

    protected String charsetName = "utf-8";

    protected String writeMode = "append";

    protected List<String> columnTypes;

    protected List<String> columnNames;

    private transient FtpHandler ftpHandler;

    private transient OutputStream os;

    private Row lastRow;

    private long rowsOfCurrentFile;

    private boolean readyCheckpoint;

    private int fileIndex;

    private static String tempPath = ".flinkxTmp";

    private static String finishedTagPath = tempPath + "/.finishedTag";

    private String currentFileNamePrefix;

    private String currentFileName;

    @Override
    public void configure(Configuration parameters) {
        if(SFTP_PROTOCOL.equalsIgnoreCase(protocol)) {
            ftpHandler = new SFtpHandler();
        } else {
            ftpHandler = new StandardFtpHandler();
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
            if("overwrite".equalsIgnoreCase(writeMode) && !"/".equals(path)) {
                ftpHandler.deleteAllFilesInDir(path);
            }

            if (restoreConfig.isRestore()){
                ftpHandler.deleteAllFilesInDir(path + "/" + tempPath);
            }
        }
    }

    @Override
    public void openInternal(int taskNumber, int numTasks) throws IOException {
        ftpHandler.mkDirRecursive(path);

        if (restoreConfig.isRestore()){
            ftpHandler.mkDirRecursive(path + "/" + tempPath);
            ftpHandler.mkDirRecursive(path + "/" + finishedTagPath);
        }

        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        String dateString = formatter.format(currentTime);
        currentFileNamePrefix = taskNumber + "." + dateString + "." + UUID.randomUUID() + ".csv";

        if (restoreConfig.isRestore()){
            nextFile();
        } else {
            String filePath = path + "/" + currentFileNamePrefix;
            this.os = ftpHandler.getOutputStream(filePath);
        }
    }

    private void nextFile() {
        if (os != null){
            return;
        }

        tmpToData();

        currentFileName = "." + fileIndex + currentFileNamePrefix;
        String filePath = path + "/" + tempPath + "/" + currentFileName;
        os = ftpHandler.getOutputStream(filePath);
        fileIndex++;
    }

    private void tmpToData(){
        if (currentFileName == null || !currentFileName.startsWith(".")){
            return;
        }

        try{
            String src = path + "/" + tempPath + "/" + currentFileName;

            currentFileName = currentFileName.replaceFirst("\\.", "");
            String dist = path + "/" + tempPath + "/" + currentFileName;
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
            ftpHandler.deleteAllFilesInDir(path + "/" + tempPath + "/" + currentFileName);
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
        try{
            String state = getTaskState();
            if(!RUNNING_STATE.equals(state)){
                ftpHandler.logoutFtpServer();
                return;
            }
        } catch (IOException e){
            throw new RuntimeException(e);
        }

        if (taskNumber == 0){
            final int maxRetryTime = 100;
            for (int i = 0; i < maxRetryTime; i++) {
                if(ftpHandler.getFiles(path + "/" + finishedTagPath).size() == numTasks){
                    break;
                }

                SysUtil.sleep(3000);
            }

            try{
                List<String> files = ftpHandler.getFiles(path + "/" + tempPath);
                for (String file : files) {
                    if (file.endsWith(".csv") && !file.startsWith(".")){
                        ftpHandler.rename(path + "/" + tempPath + "/" + file,path + "/" + file);
                    }
                }
            }catch (Exception e){
                throw new RuntimeException("Rename temp file error:", e);
            }

            ftpHandler.deleteAllFilesInDir(path + "/" + tempPath);
            ftpHandler.logoutFtpServer();
        }
    }

    @Override
    public void closeInternal() throws IOException {
        OutputStream s = os;
        if(s != null) {
            s.flush();
            os = null;
            s.close();
        }

        if (restoreConfig.isRestore() && ftpHandler != null){
            String finishedTag = path + "/" + finishedTagPath + "/" + taskNumber + "_finished";
            ftpHandler.mkDirRecursive(finishedTag);
        }

        tmpToData();

        if(ftpHandler != null && (!restoreConfig.isRestore() || taskNumber > 0)) {
            ftpHandler.logoutFtpServer();
        }
    }

}
