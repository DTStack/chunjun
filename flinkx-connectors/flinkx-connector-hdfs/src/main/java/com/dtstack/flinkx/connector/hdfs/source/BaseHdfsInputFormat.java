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
package com.dtstack.flinkx.connector.hdfs.source;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.hdfs.conf.HdfsConf;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.source.format.BaseRichInputFormat;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.util.FileSystemUtil;
import com.dtstack.flinkx.util.PluginUtil;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.core.io.InputSplit;

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

/**
 * Date: 2021/06/08 Company: www.dtstack.com
 *
 * @author tudou
 */
public abstract class BaseHdfsInputFormat extends BaseRichInputFormat {

    protected HdfsConf hdfsConf;

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

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws IOException {
        openKerberos = FileSystemUtil.isOpenKerberos(hdfsConf.getHadoopConfig());
        if (openKerberos) {
            DistributedCache distributedCache =
                    PluginUtil.createDistributedCacheFromContextClassLoader();
            UserGroupInformation ugi =
                    FileSystemUtil.getUGI(
                            hdfsConf.getHadoopConfig(), hdfsConf.getDefaultFS(), distributedCache);
            LOG.info("user:{}, ", ugi.getShortUserName());
            return ugi.doAs(
                    (PrivilegedAction<InputSplit[]>)
                            () -> {
                                try {
                                    return createHdfsSplit(minNumSplits);
                                } catch (Exception e) {
                                    throw new FlinkxRuntimeException(
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
        openKerberos = FileSystemUtil.isOpenKerberos(hdfsConf.getHadoopConfig());
        if (openKerberos) {
            ugi =
                    FileSystemUtil.getUGI(
                            hdfsConf.getHadoopConfig(),
                            hdfsConf.getDefaultFS(),
                            getRuntimeContext().getDistributedCache());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean reachedEnd() throws IOException {
        return !recordReader.next(key, value);
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
                FileSystemUtil.getJobConf(hdfsConf.getHadoopConfig(), hdfsConf.getDefaultFS());
        hadoopJobConf.set(HdfsPathFilter.KEY_REGEX, hdfsConf.getFilterRegex());
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

        for (FieldConf fieldConf : hdfsConf.getColumn()) {
            if (fieldConf.getPart()) {
                fieldConf.setValue(map.get(fieldConf.getName()));
            }
        }
    }

    /**
     * create hdfs data splits
     *
     * @param minNumSplits
     * @return
     * @throws IOException
     */
    public abstract InputSplit[] createHdfsSplit(int minNumSplits) throws IOException;

    /**
     * create hdfs inputFormat
     *
     * @return org.apache.hadoop.mapred.InputFormat
     */
    public abstract org.apache.hadoop.mapred.InputFormat createInputFormat();

    public void setHdfsConf(HdfsConf hdfsConf) {
        this.hdfsConf = hdfsConf;
    }
}
