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

package com.dtstack.chunjun.connector.inceptor.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.inceptor.conf.InceptorFileConf;
import com.dtstack.chunjun.connector.inceptor.enums.ECompressType;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.sink.format.BaseFileOutputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.ColumnTypeUtil;
import com.dtstack.chunjun.util.FileSystemUtil;
import com.dtstack.chunjun.util.PluginUtil;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RuntimeContext;

import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseInceptorFileOutputFormat extends BaseFileOutputFormat {
    protected InceptorFileConf inceptorFileConf;
    protected FileSystem fs;

    protected List<String> fullColumnNameList;
    protected List<String> fullColumnTypeList;
    protected Configuration conf;
    protected transient Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo;

    // 如果key为string类型的值是map 或者 list 会使用gson转为json格式存入
    protected transient Gson gson;

    protected transient UserGroupInformation ugi;
    protected boolean openKerberos;
    protected ECompressType compressType;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        // 这里休眠一段时间是为了避免reader和writer或者多个任务在同一个taskManager里同时认证kerberos
        gson = new Gson();
        openKerberos = FileSystemUtil.isOpenKerberos(inceptorFileConf.getHadoopConfig());
        // 这里休眠一段时间是为了避免reader和writer或者多个任务在同一个taskmanager里同时认证kerberos
        if (FileSystemUtil.isOpenKerberos(inceptorFileConf.getHadoopConfig())) {
            sleepRandomTime();
        }

        if (openKerberos) {
            ugi =
                    FileSystemUtil.getUGI(
                            inceptorFileConf.getHadoopConfig(),
                            inceptorFileConf.getDefaultFs(),
                            getRuntimeContext().getDistributedCache());
        } else {
            String currentUser = FileSystemUtil.getHadoopUser(inceptorFileConf.getHadoopConfig());
            this.ugi = UserGroupInformation.createRemoteUser(currentUser);
        }

        super.openInternal(taskNumber, numTasks);
    }

    @Override
    protected void initVariableFields() {
        if (CollectionUtils.isNotEmpty(inceptorFileConf.getFullColumnName())) {
            fullColumnNameList = inceptorFileConf.getFullColumnName();
        } else {
            fullColumnNameList =
                    inceptorFileConf.getColumn().stream()
                            .map(FieldConf::getName)
                            .collect(Collectors.toList());
        }

        if (CollectionUtils.isNotEmpty(inceptorFileConf.getFullColumnType())) {
            fullColumnTypeList = inceptorFileConf.getFullColumnType();
        } else {
            fullColumnTypeList =
                    inceptorFileConf.getColumn().stream()
                            .map(FieldConf::getType)
                            .collect(Collectors.toList());
        }
        super.initVariableFields();
    }

    @Override
    protected void checkOutputDir() {
        if (inceptorFileConf.isTransaction()) {
            return;
        }
        Path dir = new Path(tmpPath);
        if (fs == null) {
            openSource();
        }

        ugi.doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            if (fs.exists(dir)) {
                                if (fs.isFile(dir)) {
                                    throw new ChunJunRuntimeException(
                                            String.format("dir:[%s] is a file", tmpPath));
                                }
                            } else {
                                fs.mkdirs(dir);
                            }
                        } catch (IOException e) {
                            throw new ChunJunRuntimeException(
                                    "cannot check or create temp directory: " + tmpPath, e);
                        }
                        return null;
                    }
                });
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
        DistributedCache distributedCache = getDistributedCache();

        if (ugi == null) {
            try {
                openKerberos = FileSystemUtil.isOpenKerberos(inceptorFileConf.getHadoopConfig());
                // 这里休眠一段时间是为了避免reader和writer或者多个任务在同一个taskmanager里同时认证kerberos
                if (openKerberos) {
                    sleepRandomTime();
                }

                if (openKerberos) {
                    ugi =
                            FileSystemUtil.getUGI(
                                    inceptorFileConf.getHadoopConfig(),
                                    inceptorFileConf.getDefaultFs(),
                                    distributedCache);

                } else {
                    String currentUser =
                            FileSystemUtil.getHadoopUser(inceptorFileConf.getHadoopConfig());
                    this.ugi = UserGroupInformation.createRemoteUser(currentUser);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        conf =
                FileSystemUtil.getConfiguration(
                        inceptorFileConf.getHadoopConfig(), inceptorFileConf.getDefaultFs());

        ugi.doAs(
                new PrivilegedAction<FileSystem>() {
                    @Override
                    public FileSystem run() {
                        try {
                            fs =
                                    FileSystemUtil.getFileSystem(
                                            inceptorFileConf.getHadoopConfig(),
                                            inceptorFileConf.getDefaultFs(),
                                            distributedCache);
                            return null;
                        } catch (Exception e) {
                            throw new ChunJunRuntimeException("can't init fileSystem", e);
                        }
                    }
                });
    }

    @Override
    public String getExtension() {
        return compressType.getSuffix();
    }

    @Override
    protected long getCurrentFileSize() {
        String path = tmpPath + File.separatorChar + currentFileName;
        try {
            if (inceptorFileConf.getMaxFileSize() > ConstantValue.STORE_SIZE_G) {
                return fs.getFileStatus(new Path(path)).getLen();
            } else {
                return fs.open(new Path(path)).available();
            }
        } catch (IOException e) {
            throw new ChunJunRuntimeException("can't get file size from hdfs, file = " + path, e);
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
                currentFilePath = path + File.separatorChar + fileName;
                Path commitFilePath = new Path(currentFilePath);
                fs.delete(commitFilePath, true);
                LOG.info("delete file:{}", currentFilePath);
            }
        } catch (IOException e) {
            throw new ChunJunRuntimeException(
                    String.format("can't delete commit file:[%s]", currentFilePath), e);
        }
    }

    @Override
    protected void moveAllTmpDataFileToDir() {
        if (inceptorFileConf.isTransaction()) {
            return;
        }
        if (fs == null) {
            openSource();
        }
        String currentFilePath = "";
        try {
            Path dir = new Path(outputFilePath);
            Path tmpDir = new Path(tmpPath);

            FileStatus[] dataFiles = fs.listStatus(tmpDir);
            for (FileStatus dataFile : dataFiles) {
                currentFilePath = dataFile.getPath().getName();
                fs.rename(dataFile.getPath(), dir);
                LOG.info("move temp file:{} to dir:{}", dataFile.getPath(), dir);
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
     * @return
     */
    protected abstract ECompressType getCompressType();

    protected void deleteDirectory(String path) {
        LOG.info("start to delete directory：{}", path);
        try {
            Path dir = new Path(path);
            if (fs == null) {
                openSource();
            }
            if (fs.exists(dir)) {
                if (fs.isFile(dir)) {
                    throw new ChunJunRuntimeException(String.format("dir:[%s] is a file", path));
                } else {
                    fs.delete(dir, true);
                }
            }
        } catch (IOException e) {
            throw new ChunJunRuntimeException("cannot delete directory: " + path, e);
        }
    }

    public void setInceptorFileConf(InceptorFileConf inceptorFileConf) {
        this.inceptorFileConf = inceptorFileConf;
    }

    public void setCompressType(ECompressType compressType) {
        this.compressType = compressType;
    }

    public InceptorFileConf getInceptorFileConf() {
        return inceptorFileConf;
    }

    private void sleepRandomTime() {
        try {
            Thread.sleep(5000L + (long) (10000 * Math.random()));
        } catch (Exception exception) {
            LOG.warn("", exception);
        }
    }

    protected DistributedCache getDistributedCache() {
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
        return distributedCache;
    }
}
