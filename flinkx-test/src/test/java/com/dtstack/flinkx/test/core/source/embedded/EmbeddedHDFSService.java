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


package com.dtstack.flinkx.test.core.source.embedded;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.flinkx.test.core.source.HdfsDataSource;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 创建启动HDFS服务
 *
 * @author jiangbo
 * @date 2020/2/29
 */
public class EmbeddedHDFSService {

    public static Logger LOG = LoggerFactory.getLogger(HdfsDataSource.class);

    public EmbeddedHDFSService() {

    }

    public JSONObject startService() {
        JSONObject hadoopConfig = new JSONObject();
        hadoopConfig.put("fs.hdfs.impl.disable.cache", true);
        hadoopConfig.put("hadoop.user.name", System.getProperty("user.name"));

        try {
            HdfsConfiguration hadoopConf = new HdfsConfiguration();

            File testDataPath = new File(PathUtils.getTestDir(this.getClass()), "miniclusters");
            File testDataCluster1 = new File(testDataPath, "cluster1");
            String c1Path = testDataCluster1.getAbsolutePath();
            hadoopConf.set("hdfs.minidfs.basedir", c1Path);

            MiniDFSCluster miniDFS = new MiniDFSCluster.Builder(hadoopConf).build();

            String defaultFs = miniDFS.getFileSystem().getUri().toString();
            LOG.warn("启动HDFS成功:{}", defaultFs);

            hadoopConfig.put("fs.default.name", defaultFs);
        } catch (Exception e) {
            LOG.warn("启动HDFS服务失败", e);
        }

        return hadoopConfig;
    }
}
