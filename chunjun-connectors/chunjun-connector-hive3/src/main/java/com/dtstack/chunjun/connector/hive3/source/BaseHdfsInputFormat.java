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

package com.dtstack.chunjun.connector.hive3.source;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.hive3.conf.HdfsConf;
import com.dtstack.chunjun.connector.hive3.util.Hive3Util;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.PluginUtil;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.core.io.InputSplit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

/** @author liuliu 2022/3/23 */
public abstract class BaseHdfsInputFormat extends BaseRichInputFormat {

    protected HdfsConf hdfsConf;

    /** the key to read data into */
    protected Object key;
    /** the value to read data into */
    protected Object value;

    protected boolean openKerberos;

    protected transient UserGroupInformation ugi;
    protected transient org.apache.hadoop.mapred.InputFormat inputFormat;
    protected transient JobConf hadoopJobConf;
    protected transient RecordReader recordReader;

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        if (Hive3Util.isOpenKerberos(hdfsConf.getHadoopConfig())) {
            DistributedCache distributedCache =
                    PluginUtil.createDistributedCacheFromContextClassLoader();
            UserGroupInformation ugi =
                    Hive3Util.getUGI(
                            hdfsConf.getHadoopConfig(), hdfsConf.getDefaultFS(), distributedCache);
            return ugi.doAs(
                    (PrivilegedExceptionAction<InputSplit[]>)
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

    /** init Hadoop Job Config */
    protected void initHadoopJobConf() {
        hadoopJobConf = Hive3Util.getJobConf(hdfsConf.getHadoopConfig(), hdfsConf.getDefaultFS());
        hadoopJobConf.set(HdfsPathFilter.KEY_REGEX, hdfsConf.getFilterRegex());
        Hive3Util.setHadoopUserName(hadoopJobConf);
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        initHadoopJobConf();
        this.inputFormat = createMapredInputFormat();
        openKerberos = Hive3Util.isOpenKerberos(hdfsConf.getHadoopConfig());
        if (openKerberos) {
            ugi =
                    Hive3Util.getUGI(
                            hdfsConf.getHadoopConfig(),
                            hdfsConf.getDefaultFS(),
                            getRuntimeContext().getDistributedCache());
        }
    }

    protected abstract InputSplit[] createHdfsSplit(int minNumSplits) throws IOException;

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

    /**
     * 从hdfs路径中获取当前分区信息
     *
     * @param path hdfs路径
     */
    public void findCurrentPartition(Path path) {
        if (null == path) {
            LOG.warn("The Path finding partition value is null");
            return;
        }
        Map<String, String> partitionAndValueMap = new HashMap<>(16);
        String fileParentPath = path.getParent().toString();
        // 为了给下面分区列找到分区字段，将文件的父路径切分，缓存到 map 中。
        final String[] pathNodes = fileParentPath.split("/");
        for (String pathNode : pathNodes) {
            if (pathNode.contains("=") && pathNode.length() >= 3) {
                final String[] partitionAndValue = pathNode.split("=");
                // 分区 和 分区值 放入 map 中， eg : pt=20210906
                if (partitionAndValue.length == 2) {
                    partitionAndValueMap.put(partitionAndValue[0], partitionAndValue[1]);
                }
            }
        }
        // 从 map 里面找出对应分区字段，然后给该列设置值。
        for (FieldConf fieldConf : hdfsConf.getColumn()) {
            // 如果此列是分区字段
            if (fieldConf.getPart()) {
                fieldConf.setValue(partitionAndValueMap.get(fieldConf.getName()));
            }
        }
    }

    public abstract org.apache.hadoop.mapred.InputFormat createMapredInputFormat();

    public void sethdfsConf(HdfsConf hdfsConf) {
        this.hdfsConf = hdfsConf;
    }
}
