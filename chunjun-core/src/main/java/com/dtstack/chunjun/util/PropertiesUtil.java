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
package com.dtstack.chunjun.util;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesUtil {

    /**
     * 解析properties json字符串
     *
     * @param confStr json字符串
     * @return Properties
     * @throws Exception
     */
    public static Properties parseConf(String confStr) throws Exception {
        if (StringUtils.isEmpty(confStr)) {
            return new Properties();
        }

        confStr = URLDecoder.decode(confStr, Charsets.UTF_8.toString());
        return GsonUtil.GSON.fromJson(confStr, Properties.class);
    }

    public static Map<String, String> confToMap(String confStr) {
        if (StringUtils.isEmpty(confStr)) {
            return new HashMap<>();
        }

        try {
            confStr = URLDecoder.decode(confStr, Charsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            throw new ChunJunRuntimeException(e);
        }
        return GsonUtil.GSON.fromJson(
                confStr, new TypeToken<HashMap<String, String>>() {}.getType());
    }

    /**
     * Properties key value去空格
     *
     * @param confProperties
     * @return
     */
    public static Properties propertiesTrim(Properties confProperties) {
        Properties properties = new Properties();
        confProperties.forEach((k, v) -> properties.put(k.toString().trim(), v.toString().trim()));
        return properties;
    }

    /**
     * 初始化CommonConfig
     *
     * @param commonConfig
     * @param syncConfig
     */
    public static void initCommonConf(CommonConfig commonConfig, SyncConfig syncConfig) {
        commonConfig.setSpeedBytes(syncConfig.getSpeed().getBytes());
        commonConfig.setSavePointPath(syncConfig.getSavePointPath());
        if (syncConfig.getMetricPluginConf() != null) {
            commonConfig.setMetricPluginRoot(
                    syncConfig.getRemotePluginPath() == null
                            ? syncConfig.getPluginRoot() + File.separator + "metrics"
                            : syncConfig.getRemotePluginPath());
            commonConfig.setMetricPluginName(syncConfig.getMetricPluginConf().getPluginName());
            commonConfig.setMetricProps(syncConfig.getMetricPluginConf().getPluginProp());
            commonConfig.setRowSizeCalculatorType(
                    syncConfig.getMetricPluginConf().getRowSizeCalculatorType());
        }
    }
}
