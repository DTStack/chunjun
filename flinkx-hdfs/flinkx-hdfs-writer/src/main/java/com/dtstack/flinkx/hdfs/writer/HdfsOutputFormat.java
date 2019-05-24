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

import com.dtstack.flinkx.hdfs.HdfsUtil;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.SysUtil;
import com.dtstack.flinkx.util.URLUtil;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.HttpClientBuilder;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;


/**
 * The Hdfs implementation of OutputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class HdfsOutputFormat extends RichOutputFormat {

    protected int rowGroupSize;

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

    protected int blockIndex = 0;

    protected boolean readyCheckpoint;

    protected Row lastRow;

    protected String currentBlockFileNamePrefix;

    protected String currentBlockFileName;

    protected long rowsOfCurrentBlock;

    protected  long maxFileSize;

    protected long lastWriteSize;

    private String taskId;

    private CloseableHttpClient httpClient;

    private transient final Gson gson = new Gson();

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
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        String dateString = formatter.format(currentTime);
        currentBlockFileNamePrefix = taskNumber + "." + dateString;

        tmpPath = outputFilePath + SP + DATA_SUBDIR + SP + jobId;
        finishedPath = outputFilePath + SP + FINISHED_SUBDIR + SP + jobId + SP + taskNumber;

        httpClient = HttpClientBuilder.create().build();
        open();
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

                formatState.setState(lastRow.getField(restoreConfig.getRestoreColumnIndex()));
                formatState.setNumberWrite(numWriteCounter.getLocalValue());
                numWriteCounter.add(rowsOfCurrentBlock);
                rowsOfCurrentBlock = 0;

                return formatState;
            }

            return null;
        }catch (Exception e){
            try{
                fs.delete(new Path(tmpPath + SP + currentBlockFileName), true);
                fs.close();
            }catch (Exception e1){
                throw new RuntimeException("Delete tmp file:" + currentBlockFileName + " failed when get next block error", e1);
            }

            throw new RuntimeException("Get next block error:", e);
        }
    }

    protected void tmpToData(){
        try {
            if (currentBlockFileName != null && currentBlockFileName.startsWith(".")){
                Path src = new Path(tmpPath + SP + currentBlockFileName);

                String dataFileName = currentBlockFileName.replaceFirst("\\.","");
                Path dist = new Path(tmpPath + SP + dataFileName);

                fs.rename(src, dist);
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    protected abstract void open() throws IOException;

    protected abstract void nextBlock();

    protected abstract void flushBlock() throws IOException;

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        // CAN NOT HAPPEN
    }

    @Override
    public void tryCleanupOnError() throws Exception {
        if(!restoreConfig.isRestore() && fs != null) {
            cleanTempDir();
        }
    }

    private void cleanTempDir() throws Exception{
        Path finishedDir = new Path(outputFilePath + SP + FINISHED_SUBDIR);
        fs.delete(finishedDir, true);
        LOG.info("Delete .finished dir");

        Path tmpDir = new Path(outputFilePath + SP + DATA_SUBDIR);
        fs.delete(tmpDir, true);
        LOG.info("Delete .data dir");
    }

    @Override
    protected void afterCloseInternal()  {
        try {
            // 判断任务是不是正常结束
            String state = getTaskState();
            if(!RUNNING_STATE.equals(state)){
                if (!restoreConfig.isRestore()){
                    cleanTempDir();
                }
                fs.close();
                return;
            }

            // write finished file
            fs.createNewFile(new Path(finishedPath));

            // task_0 move tmp data into destination
            if(taskNumber == 0) {
                Path finishedDir = new Path(outputFilePath + SP + FINISHED_SUBDIR + SP + jobId);
                final int maxRetryTime = 100;
                int i = 0;
                for(; i < maxRetryTime; ++i) {
                    if(fs.listStatus(finishedDir).length == numTasks) {
                        break;
                    }
                    SysUtil.sleep(3000);
                }

                if (i == maxRetryTime) {
                    fs.delete(new Path(outputFilePath + SP + DATA_SUBDIR + SP + jobId), true);
                    fs.delete(finishedDir, true);
                    throw new RuntimeException("timeout when gathering finish tags for each subtasks");
                }

                PathFilter pathFilter = path -> !path.getName().startsWith(".");

                // 不是追加模式，清除目录
                Path dir = new Path(outputFilePath);
                if(!"APPEND".equalsIgnoreCase(writeMode)){
                    if(fs.exists(dir)) {
                        FileStatus[] dataFiles = fs.listStatus(dir, pathFilter);
                        for(FileStatus dataFile : dataFiles) {
                            fs.delete(dataFile.getPath(), true);
                        }
                        fs.mkdirs(dir);
                    }
                }

                Path tmpDir = new Path(outputFilePath + SP + DATA_SUBDIR + SP + jobId);
                FileStatus[] dataFiles = fs.listStatus(tmpDir, pathFilter);
                for(FileStatus dataFile : dataFiles) {
                    fs.rename(dataFile.getPath(), dir);
                }

                cleanTempDir();
            }
            fs.close();
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }

    }

    /**
     * Get the rate of compress
     * @return
     */
    protected abstract double getCompressRate();

    protected void checkWriteSize() {
        if(StringUtils.isBlank(monitorUrl)){
            return;
        }

        if(rowsOfCurrentBlock < 5000){
            return;
        }

        long writeSize = getWriteByteSizeOfThisSubTask();
        if (getCompressRate() * (writeSize - lastWriteSize) > maxFileSize){
            try{
                flushBlock();
            }catch (IOException e){
                throw new RuntimeException(e);
            }

            nextBlock();
            rowsOfCurrentBlock = 0;
        }

        lastWriteSize = writeSize;
    }

    private long getWriteByteSizeOfThisSubTask(){
        if(taskId == null){
            StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
            Map<String, String> vars = context.getMetricGroup().getAllVariables();
            taskId = vars.get("<task_id>");
        }

        String[] monitorUrls = monitorUrl.split(",");
        String requestUrl = monitorUrls[0] + "/jobs/" + jobId + "/vertices/" + taskId;

        try{
            String response = URLUtil.get(httpClient, requestUrl);
            Map<String, Object> map = gson.fromJson(response, Map.class);
            List<LinkedTreeMap> list = (List<LinkedTreeMap>) map.get("subtasks");

            for (int i = 0; i < list.size(); ++i) {
                LinkedTreeMap subTask = list.get(i);
                LinkedTreeMap subTaskMetrics = (LinkedTreeMap) subTask.get("metrics");
                if (i == taskNumber) {
                    return (long) subTaskMetrics.get("write-bytes");
                }
            }
        }catch (Exception e){
            LOG.error("Get write size of subtask:" + taskNumber + " error:",e);
        }

        return 0;
    }

    @Override
    protected boolean needWaitAfterCloseInternal() {
        return true;
    }

}
