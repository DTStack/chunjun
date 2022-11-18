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

package com.dtstack.chunjun.util;

import com.dtstack.chunjun.enums.ClusterMode;
import com.dtstack.chunjun.enums.EPluginLoadMode;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;
import java.util.List;

public class ExecuteProcessHelper {

    private static final String CLASS_FILE_NAME_FMT = "class_path_%d";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * 非local模式或者shipfile部署模式，remoteSqlPluginPath必填
     *
     * @param remoteSqlPluginPath
     * @param deployMode
     * @param pluginLoadMode
     * @return
     */
    public static boolean checkRemoteSqlPluginPath(
            String remoteSqlPluginPath, String deployMode, String pluginLoadMode) {
        if (StringUtils.isEmpty(remoteSqlPluginPath)) {
            return StringUtils.equalsIgnoreCase(pluginLoadMode, EPluginLoadMode.SHIPFILE.name())
                    || StringUtils.equalsIgnoreCase(deployMode, ClusterMode.local.name())
                    || StringUtils.equalsIgnoreCase(deployMode, ClusterMode.localTest.name());
        }
        return true;
    }

    public static List<URL> getExternalJarUrls(String addJarListStr) throws java.io.IOException {
        List<URL> jarUrlList = Lists.newArrayList();
        if (Strings.isNullOrEmpty(addJarListStr)) {
            return jarUrlList;
        }

        List<String> addJarFileList =
                OBJECT_MAPPER.readValue(
                        URLDecoder.decode(addJarListStr, Charsets.UTF_8.name()), List.class);
        // Get External jar to load
        for (String addJarPath : addJarFileList) {
            jarUrlList.add(new File(addJarPath).toURI().toURL());
        }
        return jarUrlList;
    }
}
