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

    private String fileName;

    private int fileIndex;

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
        }
    }

    @Override
    public void openInternal(int taskNumber, int numTasks) throws IOException {
        ftpHandler.mkDirRecursive(path);
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        String dateString = formatter.format(currentTime);
        fileName = taskNumber + "." + dateString + "." + UUID.randomUUID() + ".csv";

        if (restoreConfig.isRestore()){
            nextFile();
        } else {
            String filePath = path + "/" + fileName;
            this.os = ftpHandler.getOutputStream(filePath);
        }
    }

    private void nextFile() throws IOException{
        if (os != null){
            os.flush();
            os = null;
        }

        String filePath = path + "/" + fileIndex + "." + fileName;
        os = ftpHandler.getOutputStream(filePath);
        fileIndex++;
    }

    @Override
    public FormatState getFormatState() {
        try{
            if (readyCheckpoint || rowsOfCurrentFile > restoreConfig.getMaxRowNumForCheckpoint()){
                nextFile();

                rowsOfCurrentFile = 0;
                formatState.setState(lastRow.getField(restoreConfig.getRestoreColumnIndex()));
                return formatState;
            }

            return null;
        }catch (Exception e){
            throw new RuntimeException("Get next file error:", e);
        }
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {

        if (restoreConfig.isRestore() && lastRow != null){
            readyCheckpoint = ObjectUtils.equals(lastRow.getField(restoreConfig.getRestoreColumnIndex()),
                    row.getField(restoreConfig.getRestoreColumnIndex()));
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
    public void closeInternal() throws IOException {
        OutputStream s = os;
        if(s != null) {
            s.flush();
            os = null;
            s.close();
        }
        if(ftpHandler != null) {
            ftpHandler.logoutFtpServer();
        }
    }

}
