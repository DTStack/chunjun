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

package com.dtstack.chunjun.restore.mysql.datasource;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.google.common.collect.Maps;

import javax.sql.DataSource;

import java.util.Map;
import java.util.Properties;

public class DruidDataSourceManager {

    private static final Map<String, DataSource> PROVIDER_MAP = Maps.newHashMap();

    private DruidDataSourceManager() throws IllegalAccessException {
        throw new IllegalAccessException(getClass() + " can not be instantiated.");
    }

    public static void register(String url, DataSource provider) {
        PROVIDER_MAP.put(url, provider);
    }

    public static DataSource get(String url) {
        return PROVIDER_MAP.get(url);
    }

    public static boolean contains(String url) {
        return PROVIDER_MAP.containsKey(url);
    }

    public static DataSource create(Properties properties) throws Exception {
        String url = properties.getProperty(DruidDataSourceFactory.PROP_URL);

        if (PROVIDER_MAP.containsKey(url)) {
            return PROVIDER_MAP.get(url);
        }

        DataSource dataSource = DruidDataSourceFactory.createDataSource(properties);

        PROVIDER_MAP.put(url, dataSource);

        return dataSource;
    }
}
