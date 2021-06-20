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
package com.dtstack.flinkx.connector.hdfs.sink;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.hdfs.CompressType;
import com.dtstack.flinkx.connector.hdfs.conf.HdfsConf;
import com.dtstack.flinkx.outputformat.BaseFileOutputFormat;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.util.ColumnTypeUtil;
import com.dtstack.flinkx.util.FileSystemUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Date: 2021/06/09 Company: www.dtstack.com
 *
 * @author tudou
 */
public abstract class BaseHdfsOutputFormat extends BaseFileOutputFormat {

    private static final int FILE_NAME_PART_SIZE = 3;

    protected FileSystem fs;
    protected HdfsConf hdfsConf;

    protected List<String> fullColumnNameList;
    protected List<String> fullColumnTypeList;
    protected int[] colIndices;
    protected Configuration conf;
    protected transient Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo;
    protected CompressType compressType;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        // 这里休眠一段时间是为了避免reader和writer或者多个任务在同一个taskManager里同时认证kerberos
        if (FileSystemUtil.isOpenKerberos(hdfsConf.getHadoopConfig())) {
            try {
                Thread.sleep(5000L + (long) (10000 * Math.random()));
            } catch (Exception e) {
                LOG.warn("", e);
            }
        }

        if (CollectionUtils.isNotEmpty(hdfsConf.getFullColumnName())) {
            fullColumnNameList = hdfsConf.getFullColumnName();
        }else{
            fullColumnNameList = hdfsConf.getColumn().stream().map(FieldConf::getName).collect(Collectors.toList());
        }

        if (CollectionUtils.isNotEmpty(hdfsConf.getFullColumnType())) {
            fullColumnTypeList = hdfsConf.getFullColumnType();
        }else{
            fullColumnTypeList = hdfsConf.getColumn().stream().map(FieldConf::getType).collect(Collectors.toList());
        }
        colIndices = new int[hdfsConf.getColumn().size()];
        for(int i = 0; i < hdfsConf.getColumn().size(); ++i) {
            int j = 0;
            for(; j < fullColumnNameList.size(); ++j) {
                if(hdfsConf.getColumn().get(i).getName().equalsIgnoreCase(fullColumnNameList.get(j))) {
                    colIndices[i] = j;
                    break;
                }
            }
            if(j == hdfsConf.getColumn().size()) {
                colIndices[i] = -1;
            }
        }
        compressType = getCompressType();
        super.openInternal(taskNumber, numTasks);
    }

    @Override
    protected void checkOutputDir() {
        try {
            Path dir = new Path(tmpPath);
            if(fs == null){
                openSource();
            }
            if (fs.exists(dir)) {
                if (fs.isFile(dir)) {
                    throw new FlinkxRuntimeException(String.format("dir:[%s] is a file", tmpPath));
                }
            } else {
                fs.mkdirs(dir);
            }
        } catch (IOException e) {
            throw new FlinkxRuntimeException("cannot check or create temp directory: " + tmpPath, e);
        }
    }

    @Override
    protected void deleteDataDir() {
        deleteDirectory(outputFilePath);
    }

    @Override
    protected void deleteTmpDataDir() {
        deleteDirectory(tmpPath);
    }

    @Override
    protected void openSource(){
        try {
            conf = FileSystemUtil.getConfiguration(hdfsConf.getHadoopConfig(), hdfsConf.getDefaultFS());
            fs = FileSystemUtil.getFileSystem(hdfsConf.getHadoopConfig(), hdfsConf.getDefaultFS());
        } catch (Exception e) {
            throw new FlinkxRuntimeException("can't init fileSystem", e);
        }
    }

    @Override
    public String getExtension() {
        return compressType.getSuffix();
    }

    @Override
    protected long getCurrentFileSize(){
        String path = tmpPath + currentFileName;
        try {
            return fs.getFileStatus(new Path(path)).getLen();
        }catch (IOException e){
            throw new FlinkxRuntimeException("can't get file size from hdfs, file = " + path, e);
        }
    }

    @Override
    protected List<String> copyTmpDataFileToDir() {
        String filePrefix = jobId + "_" + taskNumber;
        PathFilter pathFilter = path -> path.getName().startsWith(filePrefix);
        Path dir = new Path(outputFilePath);
        Path tmpDir = new Path(tmpPath);
        String currentFilePath = "";
        List<String> copyList = new ArrayList<>();
        try {
            FileStatus[] dataFiles = fs.listStatus(tmpDir, pathFilter);
            for (FileStatus dataFile : dataFiles) {
                currentFilePath = dataFile.getPath().getName();
                FileUtil.copy(fs, dataFile.getPath(), fs, dir, false, conf);
                copyList.add(currentFilePath);
                LOG.info("copy temp file:{} to dir:{}", currentFilePath, dir);
            }
        }catch (Exception e){
            throw new FlinkxRuntimeException(String.format("can't copy temp file:[%s] to dir:[%s]", currentFilePath, outputFilePath), e);
        }
        return copyList;
    }

    @Override
    protected void deleteTmpDataFiles() {
        String filePrefix = jobId + "_" + taskNumber;
        PathFilter pathFilter = path -> path.getName().startsWith(filePrefix);
        Path tmpDir = new Path(tmpPath);
        String currentFilePath = "";
        try {
            FileStatus[] dataFiles = fs.listStatus(tmpDir, pathFilter);
            for (FileStatus dataFile : dataFiles) {
                currentFilePath = dataFile.getPath().getName();
                fs.delete(dataFile.getPath(), true);
                LOG.info("delete temp file:{}", currentFilePath);
            }
        }catch (Exception e){
            throw new FlinkxRuntimeException(String.format("can't delete temp file:[%s]", currentFilePath), e);
        }
    }

    @Override
    protected void deleteDirDataFiles(List<String> preCommitFilePathList) {
        String currentFilePath = "";
        try {
            for (String path : this.preCommitFilePathList) {
                currentFilePath = path;
                Path commitFilePath = new Path(path);
                fs.delete(commitFilePath, true);
                LOG.info("delete commit file:{}", path);
            }
        }catch (IOException e){
            throw new FlinkxRuntimeException(String.format("can't delete commit file:[%s]", currentFilePath), e);
        }
    }

    @Override
    protected void moveAllTmpDataFileToDir() {
        if(fs == null){
            openSource();
        }
        String currentFilePath = "";
        try {
            PathFilter pathFilter = path -> !path.getName().startsWith(jobId);
            Path dir = new Path(outputFilePath);
            Path tmpDir = new Path(tmpPath);

            FileStatus[] dataFiles = fs.listStatus(tmpDir, pathFilter);
            for (FileStatus dataFile : dataFiles) {
                currentFilePath = dataFile.getPath().getName();
                fs.rename(dataFile.getPath(), dir);
                LOG.info("move temp file:{} to dir:{}", dataFile.getPath(), dir);
            }
        } catch (IOException e) {
            throw new FlinkxRuntimeException(String.format("can't move file:[%s] to dir:[%s]", currentFilePath, outputFilePath), e);
        }
    }

    @Override
    protected void closeSource() {
        try {
            if (fs != null) {
                fs.close();
                fs = null;
            }
        }catch (IOException e){
            throw new FlinkxRuntimeException("can't close source.", e);
        }
    }

    @Override
    public float getDeviation(){
        return compressType.getDeviation();
    }

    /**
     * get file compress type
     * @return
     */
    protected abstract CompressType getCompressType();

    protected void deleteDirectory(String path){
        LOG.info("start to delete directory：{}", path);
        try {
            Path dir = new Path(path);
            if(fs == null){
                openSource();
            }
            if (fs.exists(dir)) {
                if (fs.isFile(dir)) {
                    throw new FlinkxRuntimeException(String.format("dir:[%s] is a file", path));
                }else{
                    fs.delete(dir, true);
                }
            }
        } catch (IOException e) {
            throw new FlinkxRuntimeException("cannot delete directory: " + path, e);
        }
    }
}
