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
package com.dtstack.chunjun.connector.hdfs.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.hdfs.config.HdfsConfig;
import com.dtstack.chunjun.connector.hdfs.enums.CompressType;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.sink.format.BaseFileOutputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.ColumnTypeUtil;
import com.dtstack.chunjun.util.FileSystemUtil;
import com.dtstack.chunjun.util.PluginUtil;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RuntimeContext;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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

@Slf4j
public abstract class BaseHdfsOutputFormat extends BaseFileOutputFormat {

    private static final long serialVersionUID = 6079975649960844071L;

    protected FileSystem fs;
    protected HdfsConfig hdfsConfig;

    protected List<String> fullColumnNameList;
    protected List<String> fullColumnTypeList;
    protected Configuration config;
    protected transient Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo;
    protected CompressType compressType;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        // 这里休眠一段时间是为了避免reader和writer或者多个任务在同一个taskManager里同时认证kerberos
        if (FileSystemUtil.isOpenKerberos(hdfsConfig.getHadoopConfig())) {
            try {
                Thread.sleep(5000L + (long) (10000 * Math.random()));
            } catch (Exception e) {
                log.warn("", e);
            }
        }
        super.openInternal(taskNumber, numTasks);
    }

    @Override
    protected void initVariableFields() {
        if (CollectionUtils.isNotEmpty(hdfsConfig.getFullColumnName())) {
            fullColumnNameList = hdfsConfig.getFullColumnName();
        } else {
            fullColumnNameList =
                    hdfsConfig.getColumn().stream()
                            .map(FieldConfig::getName)
                            .collect(Collectors.toList());
            hdfsConfig.setFullColumnName(fullColumnNameList);
        }

        if (CollectionUtils.isNotEmpty(hdfsConfig.getFullColumnType())) {
            List<String> fullColumnType = hdfsConfig.getFullColumnType();
        } else {
            fullColumnTypeList =
                    hdfsConfig.getColumn().stream()
                            .map(fieldConfig -> fieldConfig.getType().getType())
                            .collect(Collectors.toList());
            hdfsConfig.setFullColumnType(fullColumnTypeList);
        }
        compressType = getCompressType();
        super.initVariableFields();
    }

    @Override
    protected void checkOutputDir() {
        try {
            Path dir = new Path(tmpPath);
            if (fs == null) {
                openSource();
            }
            if (fs.exists(dir)) {
                if (fs.getFileStatus(dir).isFile()) {
                    throw new ChunJunRuntimeException(String.format("dir:[%s] is a file", tmpPath));
                }
            } else {
                fs.mkdirs(dir);
            }
        } catch (IOException e) {
            throw new ChunJunRuntimeException(
                    "cannot check or create temp directory: " + tmpPath, e);
        }
    }

    /** 文件分隔符(File.separatorChar)在windows为\，而在linux中为/，在hadoop中路径需要固定为/， */
    protected char getHdfsPathChar() {
        // hadoop 文件系统固定为/，避免路径不对，文件写入错误及移动失败
        return '/';
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
    protected void openSource() {
        config =
                FileSystemUtil.getConfiguration(
                        hdfsConfig.getHadoopConfig(), hdfsConfig.getDefaultFS());
        RuntimeContext runtimeContext = null;
        try {
            runtimeContext = getRuntimeContext();
        } catch (IllegalStateException e) {
            // ignore
        }
        DistributedCache distributedCache;
        if (runtimeContext == null) {
            distributedCache = PluginUtil.createDistributedCacheFromContextClassLoader();
        } else {
            distributedCache = runtimeContext.getDistributedCache();
        }
        try {
            fs =
                    FileSystemUtil.getFileSystem(
                            hdfsConfig.getHadoopConfig(),
                            hdfsConfig.getDefaultFS(),
                            distributedCache,
                            jobId,
                            String.valueOf(taskNumber));
        } catch (Exception e) {
            throw new ChunJunRuntimeException("can't init fileSystem", e);
        }
    }

    @Override
    public String getExtension() {
        return compressType.getSuffix();
    }

    @Override
    protected long getCurrentFileSize() {
        String path = tmpPath + getHdfsPathChar() + currentFileName;
        int retryTimes = 1;
        while (retryTimes <= 3) {
            try {
                if (hdfsConfig.getMaxFileSize() > ConstantValue.STORE_SIZE_G) {
                    return fs.getFileStatus(new Path(path)).getLen();
                } else {
                    return fs.open(new Path(path)).available();
                }
            } catch (IOException e) {
                if (++retryTimes <= 3) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignore) {
                    }
                } else {
                    throw new ChunJunRuntimeException(
                            "can't get file size from hdfs, file = " + path, e);
                }
            }
        }
        return 0;
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
                FileUtil.copy(fs, dataFile.getPath(), fs, dir, false, config);
                copyList.add(currentFilePath);
                log.info("copy temp file:{} to dir:{}", currentFilePath, dir);
            }
        } catch (Exception e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "can't copy temp file:[%s] to dir:[%s]",
                            currentFilePath, outputFilePath),
                    e);
        }
        return copyList;
    }

    @Override
    protected void deleteDataFiles(List<String> preCommitFilePathList, String path) {
        String currentFilePath = "";
        try {
            for (String fileName : this.preCommitFilePathList) {
                currentFilePath = path + getHdfsPathChar() + fileName;
                Path commitFilePath = new Path(currentFilePath);
                fs.delete(commitFilePath, true);
                log.info("delete file:{}", currentFilePath);
            }
        } catch (IOException e) {
            throw new ChunJunRuntimeException(
                    String.format("can't delete commit file:[%s]", currentFilePath), e);
        }
    }

    @Override
    protected void moveAllTmpDataFileToDir() {
        if (fs == null) {
            openSource();
        }
        String currentFilePath = "";
        try {
            // 在目标目录下生成一个指定文件名的空文件，例如：_SUCCESS 空文件
            if (StringUtils.isNotBlank(hdfsConfig.getFinishedFileName())) {
                String finishedFilePath =
                        tmpPath + getHdfsPathChar() + hdfsConfig.getFinishedFileName();
                fs.create(new Path(finishedFilePath), true);
                log.info("Committed with finished file:{}", finishedFilePath);
            }
            Path dir = new Path(outputFilePath);
            Path tmpDir = new Path(tmpPath);

            FileStatus[] dataFiles = fs.listStatus(tmpDir);
            for (FileStatus dataFile : dataFiles) {
                currentFilePath = dataFile.getPath().getName();
                fs.rename(dataFile.getPath(), dir);
                log.info("move temp file:{} to dir:{}", dataFile.getPath(), dir);
            }
            fs.delete(tmpDir, true);
        } catch (IOException e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "can't move file:[%s] to dir:[%s]", currentFilePath, outputFilePath),
                    e);
        }
    }

    @Override
    protected void closeSource() {
        try {
            if (fs != null) {
                fs.close();
                fs = null;
            }
        } catch (IOException e) {
            throw new ChunJunRuntimeException("can't close source.", e);
        }
    }

    @Override
    public float getDeviation() {
        return compressType.getDeviation();
    }

    /**
     * get file compress type
     *
     * @return compress type.
     */
    protected abstract CompressType getCompressType();

    protected void deleteDirectory(String path) {
        log.info("start to delete directory：{}", path);
        try {
            Path dir = new Path(path);
            if (fs == null) {
                openSource();
            }
            if (fs.exists(dir)) {
                if (fs.getFileStatus(dir).isFile()) {
                    throw new ChunJunRuntimeException(String.format("dir:[%s] is a file", path));
                } else {
                    fs.delete(dir, true);
                }
            }
        } catch (IOException e) {
            throw new ChunJunRuntimeException("cannot delete directory: " + path, e);
        }
    }

    public HdfsConfig getHdfsConf() {
        return hdfsConfig;
    }

    public void setHdfsConf(HdfsConfig hdfsConfig) {
        this.hdfsConfig = hdfsConfig;
    }
}
