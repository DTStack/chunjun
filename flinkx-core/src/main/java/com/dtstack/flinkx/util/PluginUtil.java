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


package com.dtstack.flinkx.util;

import com.dtstack.flinkx.enums.EPluginLoadMode;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.factories.FactoryUtil;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Reason:
 * Date: 2018/6/27
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class PluginUtil {

    private static final String SP = File.separator;

    private static final String JAR_SUFFIX = ".jar";

    public static String getJarFileDirPath(String type, String sqlRootDir) {
        String jarPath = sqlRootDir + SP + type;

        checkJarFileDirPath(sqlRootDir, jarPath);

        return jarPath;
    }

    private static void checkJarFileDirPath(String sqlRootDir, String path) {
        if (sqlRootDir == null || sqlRootDir.isEmpty()) {
            throw new RuntimeException("sqlPlugin is empty !");
        }

        File jarFile = new File(path);

        if (!jarFile.exists()) {
            throw new RuntimeException(String.format("path %s not exists!!!", path));
        }
    }

    public static URL[] getPluginJarUrls(String pluginDir, String factoryIdentifier) throws MalformedURLException {
        List<URL> urlList = new ArrayList<>();

        File dirFile = new File(pluginDir);

        if (pluginDir.contains("null")) {
            return urlList.toArray(new URL[0]);
        }

        if (!dirFile.exists() || !dirFile.isDirectory()) {
            throw new RuntimeException("plugin path:" + pluginDir + "is not exist.");
        }

        File[] files = dirFile.listFiles(tmpFile -> tmpFile.isFile() && tmpFile.getName().endsWith(JAR_SUFFIX));
        if (files == null || files.length == 0) {
            throw new RuntimeException("plugin path:" + pluginDir + " is null.");
        }

        for (File file : files) {
            URL pluginJarUrl = file.toURI().toURL();
            // format只加载一个jar
            if (pluginDir.endsWith(FactoryUtil.FORMATS.key())) {
                if (file.getName().contains(factoryIdentifier)) {
                    urlList.add(pluginJarUrl);
                }
            } else {
                urlList.add(pluginJarUrl);
            }
        }

        if (urlList == null || urlList.size() == 0) {
            throw new RuntimeException("no match jar in :" + pluginDir + " directory ，factoryIdentifier is :" + factoryIdentifier);
        }

        return urlList.toArray(new URL[0]);
    }

    public static String getCoreJarFileName(String path, String prefix, String pluginLoadMode) throws Exception {
        String coreJarFileName = null;
        File pluginDir = new File(path);
        if (pluginDir.exists() && pluginDir.isDirectory()) {
            File[] jarFiles = pluginDir.listFiles((dir, name) ->
                    name.toLowerCase().startsWith(prefix) && name.toLowerCase().endsWith(".jar"));

            if (jarFiles != null && jarFiles.length > 0) {
                coreJarFileName = jarFiles[0].getName();
            }
        }

        if (StringUtils.isEmpty(coreJarFileName) && !pluginLoadMode.equalsIgnoreCase(EPluginLoadMode.LOCALTEST.name())) {
            throw new Exception("Can not find core jar file in path:" + path);
        }

        return coreJarFileName;
    }
}
