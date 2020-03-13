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


package com.dtstack.flinkx.test.core;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.flinkx.test.core.source.DataSource;
import com.dtstack.flinkx.test.core.source.DataSourceFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jiangbo
 * @date 2020/2/11
 */
public abstract class BaseTest {

    protected static final Logger LOG = LoggerFactory.getLogger(BaseTest.class);

    public static final String DEFAULT_DATA_SOURCE_CONFIG_PATH = "/src/test/resources/testCase/dataSource";

    public static final String KEY_USER_DIR = "user.dir";

    public static final String KEY_DATA_SOURCE_CONFIG_PATH = "configPath";

    protected AtomicBoolean isDataSourceValid = new AtomicBoolean(false);

    protected JSONObject connectionConfig;

    protected JSONObject actionBeforeTest;

    protected JSONObject actionAfterTest;

    @BeforeClass
    public void actionBeforeClass() {
        // 准备数据源
        prepareDataSource();

        // 检查数据源有效性
        if (isDataSourceValid()) {
            try {
                prepareData();
                isDataSourceValid.set(true);
            } catch (Exception e) {
                LOG.warn("准备数据出错:", e);
            }
        }
    }

    private void prepareDataSource() {
        String configPath = getDataSourceConfigPath();
        JSONObject dataSourceConfig = FileUtil.readJson(configPath);
        DataSource dataSource = DataSourceFactory.getDataSource(dataSourceConfig, getDataSourceName());
        connectionConfig = dataSource.prepare();
    }

    private String getDataSourceConfigPath() {
        String configPath = System.getProperty(KEY_DATA_SOURCE_CONFIG_PATH);
        if (StringUtils.isNotEmpty(configPath)) {
            return configPath + "/source.json";
        } else {
            String userDir = System.getProperty(KEY_USER_DIR);
            return userDir + DEFAULT_DATA_SOURCE_CONFIG_PATH + "/source.json";
        }
    }

    private void prepareData() throws Exception{
        readActionConfig();
        prepareDataInternal();
    }

    private void readActionConfig() {
        String userDir = System.getProperty(KEY_USER_DIR);
        String path = String.format("%s%s/%s.json", userDir, DEFAULT_DATA_SOURCE_CONFIG_PATH, getDataSourceName());
        JSONObject actionJson = FileUtil.readJson(path);
        actionBeforeTest = actionJson.getJSONObject("actionBeforeTest");
        actionAfterTest = actionJson.getJSONObject("actionAfterTest");
    }

    @BeforeMethod
    public void beforeMethod() {
        if (!isDataSourceValid.get()) {
            throw new SkipException(getDataSourceName() + "数据源无效，将跳过测试.");
        }
    }

    @AfterClass
    public void actionAfterClass() {
        if (!isDataSourceValid.get()) {
            return;
        }

        try {
            cleanData();
        } catch (Exception e) {
            LOG.warn("清里数据出错:", e);
        }
    }

    protected abstract String getDataSourceName();

    protected abstract boolean isDataSourceValid();

    protected abstract void prepareDataInternal() throws Exception;

    protected abstract void cleanData() throws Exception;
}
