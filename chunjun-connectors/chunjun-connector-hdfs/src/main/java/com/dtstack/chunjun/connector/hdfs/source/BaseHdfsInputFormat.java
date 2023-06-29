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
package com.dtstack.chunjun.connector.hdfs.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.hdfs.config.HdfsConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.FileSystemUtil;
import com.dtstack.chunjun.util.PluginUtil;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.core.io.InputSplit;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public abstract class BaseHdfsInputFormat extends BaseRichInputFormat {

    private static final long serialVersionUID = 6410279064990147152L;

    protected HdfsConfig hdfsConfig;

    /** the key to read data into */
    protected Object key;
    /** the value to read data into */
    protected Object value;

    protected boolean openKerberos;

    protected transient FileSystem fs;
    protected transient UserGroupInformation ugi;
    protected transient org.apache.hadoop.mapred.InputFormat inputFormat;
    protected transient JobConf hadoopJobConf;
    protected transient RecordReader recordReader;
    protected String currentReadFilePath;

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws IOException {
        openKerberos = FileSystemUtil.isOpenKerberos(hdfsConfig.getHadoopConfig());
        if (openKerberos) {
            DistributedCache distributedCache =
                    PluginUtil.createDistributedCacheFromContextClassLoader();
            UserGroupInformation ugi =
                    FileSystemUtil.getUGI(
                            hdfsConfig.getHadoopConfig(),
                            hdfsConfig.getDefaultFS(),
                            distributedCache,
                            jobId,
                            String.valueOf(indexOfSubTask));
            log.info("user:{}, ", ugi.getShortUserName());
            return ugi.doAs(
                    (PrivilegedAction<InputSplit[]>)
                            () -> {
                                try {
                                    return createHdfsSplit(minNumSplits);
                                } catch (Exception e) {
                                    throw new ChunJunRuntimeException(
                                            "error to create hdfs splits", e);
                                }
                            });
        } else {
            return createHdfsSplit(minNumSplits);
        }
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        initHadoopJobConf();
        this.inputFormat = createInputFormat();
        openKerberos = FileSystemUtil.isOpenKerberos(hdfsConfig.getHadoopConfig());
        if (openKerberos) {
            ugi =
                    FileSystemUtil.getUGI(
                            hdfsConfig.getHadoopConfig(),
                            hdfsConfig.getDefaultFS(),
                            getRuntimeContext().getDistributedCache(),
                            jobId,
                            String.valueOf(indexOfSubTask));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean reachedEnd() throws IOException {
        boolean reachedEnd;
        try {
            reachedEnd = !recordReader.next(key, value);
        } catch (IOException e) {
            throw new ChunJunRuntimeException(
                    e.getMessage() + ", current read file path: " + currentReadFilePath);
        }
        return reachedEnd;
    }

    @Override
    public void closeInternal() throws IOException {
        if (recordReader != null) {
            recordReader.close();
        }
    }

    /** init Hadoop Job Config */
    protected void initHadoopJobConf() {
        hadoopJobConf =
                FileSystemUtil.getJobConf(hdfsConfig.getHadoopConfig(), hdfsConfig.getDefaultFS());
        hadoopJobConf.set(HdfsPathFilter.KEY_REGEX, hdfsConfig.getFilterRegex());
        FileSystemUtil.setHadoopUserName(hadoopJobConf);
    }

    /**
     * Get current partition information from hdfs path
     *
     * @param path hdfs path
     */
    public void findCurrentPartition(Path path) {
        Map<String, String> map = new HashMap<>(16);
        String pathStr = path.getParent().toString();
        int index;
        while ((index = pathStr.lastIndexOf(ConstantValue.EQUAL_SYMBOL)) > 0) {
            int i = pathStr.lastIndexOf(File.separator);
            String name = pathStr.substring(i + 1, index);
            String value = pathStr.substring(index + 1);
            map.put(name, value);
            pathStr = pathStr.substring(0, i);
        }

        for (FieldConfig fieldConfig : hdfsConfig.getColumn()) {
            if (fieldConfig.getIsPart()) {
                fieldConfig.setValue(map.get(fieldConfig.getName()));
            }
        }
    }

    public abstract InputSplit[] createHdfsSplit(int minNumSplits) throws IOException;

    public abstract org.apache.hadoop.mapred.InputFormat createInputFormat();

    public void setHdfsConf(HdfsConfig hdfsConfig) {
        this.hdfsConfig = hdfsConfig;
    }
}
