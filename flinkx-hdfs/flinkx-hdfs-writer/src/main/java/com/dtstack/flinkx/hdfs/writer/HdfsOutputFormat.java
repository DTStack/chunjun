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
import com.dtstack.flinkx.outputformat.FileOutputFormat;
import com.dtstack.flinkx.util.SysUtil;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;


/**
 * The Hdfs implementation of OutputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class HdfsOutputFormat extends FileOutputFormat {

    protected int rowGroupSize;

    protected FileSystem fs;

    /** hdfs高可用配置 */
    protected Map<String,String> hadoopConfig;

    protected String defaultFS;

    protected List<String> columnTypes;

    protected List<String> columnNames;

    protected List<String> fullColumnNames;

    protected List<String> fullColumnTypes;

    protected String delimiter;

    protected int[] colIndices;

    protected Configuration conf;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        initColIndices();
        super.openInternal(taskNumber, numTasks);
    }

    @Override
    protected void checkOutputDir() {
        try{
            Path dir = new Path(outputFilePath);

            if(fs.exists(dir)){
                if(fs.isFile(dir)){
                    throw new RuntimeException("Can't write new files under common file: " + dir + "\n"
                            + "One can only write new files under directories");
                }
            } else {
                if(!makeDir){
                    throw new RuntimeException("Output path not exists:" + outputFilePath);
                }
            }
        } catch (IOException e){
            throw new RuntimeException("Check output path error", e);
        }
    }

    @Override
    protected void openSource() throws IOException{
        conf = HdfsUtil.getHadoopConfig(hadoopConfig, defaultFS);
        fs = FileSystem.get(conf);
    }

    private void initColIndices() {
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

    @Override
    protected void alignHistoryFiles(){
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

    @Override
    protected void clearTemporaryDataFiles() throws IOException{
        Path finishedDir = new Path(outputFilePath + SP + FINISHED_SUBDIR);
        fs.delete(finishedDir, true);
        LOG.info("Delete .finished dir:{}", finishedDir);

        Path tmpDir = new Path(outputFilePath + SP + DATA_SUBDIR);
        fs.delete(tmpDir, true);
        LOG.info("Delete .data dir:{}", tmpDir);
    }

    @Override
    protected void closeSource() throws IOException {
        if(fs != null){
            fs.close();
        }
    }

    @Override
    protected void createFinishedTag() throws IOException{
        fs.createNewFile(new Path(finishedPath));
        LOG.info("Create finished tag dir:{}", finishedPath);
    }

    @Override
    protected void waitForAllTasksToFinish() throws IOException{
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

    @Override
    protected void coverageData() throws IOException{
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

    @Override
    protected void moveTemporaryDataFileToDirectory() throws IOException{
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

}
