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

package com.dtstack.chunjun.dirty.utils;

import com.dtstack.chunjun.dirty.DirtyConfig;
import com.dtstack.chunjun.options.Options;
import com.dtstack.chunjun.throwable.NoRestartException;
import com.dtstack.chunjun.util.PropertiesUtil;

import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;

import org.apache.commons.collections.MapUtils;

import java.io.File;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class DirtyConfUtil {

    private static final String DEFAULT_TYPE = "default";

    public static final String DIRTY_CONF_PREFIX = "chunjun.dirty-data.";

    public static final String TYPE_KEY = "chunjun.dirty-data.output-type";

    public static final String MAX_ROWS_KEY = "chunjun.dirty-data.max-rows";

    public static final String MAX_FAILED_ROWS_KEY = "chunjun.dirty-data.max-collect-failed-rows";

    public static final String PRINT_INTERVAL = "chunjun.dirty-data.log.print-interval";

    public static final String DIRTY_DIR = "chunjun.dirty-data.dir";

    public static final String DIRTY_DIR_SUFFIX = "dirty-data-collector";

    public static DirtyConfig parseFromMap(Map<String, String> confMap) {
        DirtyConfig dirtyConfig = new DirtyConfig();
        Properties pluginProperties = new Properties();

        String type = String.valueOf(confMap.getOrDefault(TYPE_KEY, DEFAULT_TYPE));
        if (type.equals("jdbc")) {
            type = "mysql";
        }
        long maxConsumed = Long.parseLong(String.valueOf(confMap.getOrDefault(MAX_ROWS_KEY, "0")));
        long maxFailed =
                Long.parseLong(String.valueOf(confMap.getOrDefault(MAX_FAILED_ROWS_KEY, "0")));
        long printRate = Long.parseLong(String.valueOf(confMap.getOrDefault(PRINT_INTERVAL, "1")));
        String pluginDir = MapUtils.getString(confMap, DIRTY_DIR);

        confMap.entrySet().stream()
                .filter(
                        item ->
                                item.getKey()
                                        .toLowerCase(Locale.ROOT)
                                        .startsWith(DIRTY_CONF_PREFIX))
                .forEach(
                        item ->
                                pluginProperties.put(
                                        item.getKey()
                                                .toLowerCase(Locale.ROOT)
                                                .replaceFirst(DIRTY_CONF_PREFIX, "")
                                                .trim(),
                                        item.getValue()));

        dirtyConfig.setType(type);
        dirtyConfig.setMaxConsumed(maxConsumed < 0 ? Long.MAX_VALUE : maxConsumed);
        dirtyConfig.setMaxFailedConsumed(maxFailed < 0 ? Long.MAX_VALUE : maxFailed);
        dirtyConfig.setPrintRate(printRate <= 0 ? Long.MAX_VALUE : printRate);
        dirtyConfig.setPluginProperties(pluginProperties);
        dirtyConfig.setLocalPluginPath(pluginDir);

        return dirtyConfig;
    }

    public static DirtyConfig parse(Options options) {
        try {
            Properties properties = PropertiesUtil.parseConf(options.getConfProp());
            properties.put(
                    DIRTY_DIR, options.getChunjunDistDir() + File.separator + DIRTY_DIR_SUFFIX);
            return parse(properties);
        } catch (Exception e) {
            throw new NoRestartException(
                    String.format("Parse conf [%s] to DirtyConf failed.", options.getConfProp()),
                    e);
        }
    }

    public static DirtyConfig parse(Properties properties) {
        try {
            Map<String, String> confMap = Maps.fromProperties(properties);
            return parseFromMap(confMap);
        } catch (Exception e) {
            throw new NoRestartException(
                    String.format(
                            "Parse properties to dirtyConf failed. Properties: %s", properties),
                    e);
        }
    }
}
