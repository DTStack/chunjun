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


package com.dtstack.flinkx.test.hdfs;

import com.dtstack.flinkx.test.core.BaseTest;
import com.dtstack.flinkx.util.FileSystemUtil;
import org.apache.hadoop.fs.FileSystem;

/**
 * @author jiangbo
 * @date 2020/2/29
 */
public abstract class BaseHdfsTest extends BaseTest {

    public static final String PLUGIN_NAME = "hdfs";

    protected FileSystem fs;

    @Override
    protected String getDataSourceName() {
        return PLUGIN_NAME;
    }

    @Override
    protected boolean isDataSourceValid() {
        try {
            fs = FileSystemUtil.getFileSystem(connectionConfig, null);
            return true;
        } catch (Exception e) {
            LOG.warn("校验HDFS数据源失败", e);
        }

        return false;
    }

    @Override
    protected void cleanData() throws Exception {
        if (null != fs) {
            fs.close();
        }
    }
}
