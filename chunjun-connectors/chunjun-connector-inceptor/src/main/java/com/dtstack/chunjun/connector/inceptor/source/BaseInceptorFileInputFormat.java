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

package com.dtstack.chunjun.connector.inceptor.source;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.inceptor.conf.InceptorFileConf;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.FileSystemUtil;
import com.dtstack.chunjun.util.PluginUtil;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.core.io.InputSplit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseInceptorFileInputFormat extends BaseRichInputFormat {

    protected InceptorFileConf inceptorFileConf;

    protected boolean openKerberos;
    protected transient UserGroupInformation ugi;

    protected transient JobConf jobConf;
    protected transient RecordReader recordReader;
    protected transient org.apache.hadoop.mapred.InputFormat inputFormat;

    protected Object key;
    protected Object value;

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        initUgi();
        LOG.info("user:{}, ", ugi.getShortUserName());
        return ugi.doAs(
                (PrivilegedAction<InputSplit[]>)
                        () -> {
                            try {
                                return createInceptorSplit(minNumSplits);
                            } catch (Exception e) {
                                throw new ChunJunRuntimeException("error to create hdfs splits", e);
                            }
                        });
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        initHadoopJobConf();
        this.inputFormat = createInputFormat();
        openKerberos = FileSystemUtil.isOpenKerberos(inceptorFileConf.getHadoopConfig());
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
    /**
     * create hdfs data splits
     *
     * @param minNumSplits
     * @return
     * @throws IOException
     */
    public abstract InputSplit[] createInceptorSplit(int minNumSplits) throws IOException;

    /**
     * create hdfs inputFormat
     *
     * @return org.apache.hadoop.mapred.InputFormat
     */
    public abstract org.apache.hadoop.mapred.InputFormat createInputFormat();

    public void setInceptorFileConf(InceptorFileConf inceptorFileConf) {
        this.inceptorFileConf = inceptorFileConf;
    }

    protected void initUgi() throws IOException {
        this.openKerberos = FileSystemUtil.isOpenKerberos(inceptorFileConf.getHadoopConfig());
        if (openKerberos) {
            DistributedCache distributedCache =
                    PluginUtil.createDistributedCacheFromContextClassLoader();
            this.ugi =
                    FileSystemUtil.getUGI(
                            inceptorFileConf.getHadoopConfig(),
                            inceptorFileConf.getDefaultFs(),
                            distributedCache);
        } else {
            String currentUser = FileSystemUtil.getHadoopUser(inceptorFileConf.getHadoopConfig());
            this.ugi = UserGroupInformation.createRemoteUser(currentUser);
        }
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

        for (FieldConf fieldConf : inceptorFileConf.getColumn()) {
            if (fieldConf.getPart()) {
                fieldConf.setValue(map.get(fieldConf.getName()));
            }
        }
    }

    /** init Hadoop Job Config */
    protected void initHadoopJobConf() {
        jobConf =
                FileSystemUtil.getJobConf(
                        inceptorFileConf.getHadoopConfig(), inceptorFileConf.getDefaultFs());
        jobConf.set(InceptorPathFilter.KEY_REGEX, inceptorFileConf.getFilterRegex());
        FileSystemUtil.setHadoopUserName(jobConf);
    }
}
