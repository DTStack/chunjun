/**
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

package com.dtstack.flinkx.hdfs.writer;

import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.util.SysUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
/**
 * The Hdfs implementation of OutputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class HdfsOutputFormat extends RichOutputFormat implements CleanupWhenUnsuccessful {

    protected static final String DATA_SUBDIR = ".data";

    protected static final String FINISHED_SUBDIR = ".finished";

    protected static final String SP = "/";

    protected FileSystem fs;

    protected String outputFilePath;

    /** hdfs高可用配置 */
    protected Map<String,String> hadoopConfig;

    /** 写入模式 */
    protected String writeMode;

    /** 压缩方式 */
    protected String compress;

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

    protected String charsetName = "UTF-8";

    protected int[] colIndices;

    protected Configuration conf;

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
                if(fullColumnNames.get(i).equals(columnNames.get(j))) {
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

        conf = new Configuration();

        if(hadoopConfig != null) {
            for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
            }
        }

        conf.set("fs.default.name", defaultFS);
        conf.set("fs.hdfs.impl.disable.cache", "true");
        fs = FileSystem.get(conf);
        Path dir = new Path(outputFilePath);
        // dir不能是文件
        if(fs.exists(dir) && fs.isFile(dir)){
            throw new RuntimeException("Can't write new files under common file: " + dir + "\n"
                    + "One can only write new files under directories");
        }

        configInternal();
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        String dateString = formatter.format(currentTime);
        tmpPath = outputFilePath + SP + DATA_SUBDIR + SP + taskNumber + "." + dateString;
        finishedPath = outputFilePath + SP + FINISHED_SUBDIR + SP + taskNumber;
        open();
    }

    protected abstract void open() throws IOException;

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        // CAN NOT HAPPEN
    }

    @Override
    public void tryCleanupOnError() throws Exception {
        if(fs != null) {
            Path finishedDir = new Path(outputFilePath + SP + FINISHED_SUBDIR);
            Path tmpDir = new Path(outputFilePath + SP + DATA_SUBDIR);
            fs.delete(finishedDir, true);
            fs.delete(tmpDir, true);
        }
        LOG.info(jobName + ": tryCleanupOnError over!");
    }


    @Override
    protected void afterCloseInternal()  {
        try {
            // write finished file
            fs.createNewFile(new Path(finishedPath));

            // task_0 move tmp data into destination
            if(taskNumber == 0) {
                Path finishedDir = new Path(outputFilePath + SP + FINISHED_SUBDIR);
                final int maxRetryTime = 100;
                int i = 0;
                for(; i < maxRetryTime; ++i) {
                    if(fs.listStatus(finishedDir).length == numTasks) {
                        break;
                    }
                    SysUtil.sleep(3000);
                }

                // 等待所有subtask都执行到close方法了
                if(StringUtils.isNotBlank(monitorUrl)) {
                    SysUtil.sleep(3000);
                    System.out.println("finally acquire");
                    if(errorLimiter != null) {
                        errorLimiter.acquire();
                    }
                }

                Path dir = new Path(outputFilePath);
                Path tmpDir = new Path(outputFilePath + SP + DATA_SUBDIR);
                if (writeMode != null && writeMode.trim().length() != 0 && !writeMode.equalsIgnoreCase("APPEND")) {
                    if(fs.exists(dir)) {
                        PathFilter pathFilter = new PathFilter() {
                            @Override
                            public boolean accept(Path path) {
                                return !path.getName().startsWith(".");
                            }
                        } ;
                        FileStatus[] dataFiles = fs.listStatus(dir, pathFilter);
                        for(FileStatus dataFile : dataFiles) {
                            fs.delete(dataFile.getPath(), true);
                        }
                        fs.mkdirs(dir);
                    }
                }

                if (i == maxRetryTime) {
                    fs.delete(tmpDir, true);
                    fs.delete(finishedDir, true);
                    throw new RuntimeException("timeout when gathering finish tags for each subtasks");
                }

                FileStatus[] dataFiles = fs.listStatus(tmpDir);
                for(FileStatus dataFile : dataFiles) {
                    fs.rename(dataFile.getPath(), dir);
                }
                fs.delete(tmpDir, true);
                fs.delete(finishedDir, true);
            }
            fs.close();
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }

    }

    @Override
    protected boolean needWaitAfterCloseInternal() {
        return true;
    }

}
