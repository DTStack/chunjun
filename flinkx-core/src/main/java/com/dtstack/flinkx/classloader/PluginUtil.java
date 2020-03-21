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


package com.dtstack.flinkx.classloader;

import com.dtstack.flinkx.util.SysUtil;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

/**
 * @author jiangbo
 * @date 2019/10/21
 */
public class PluginUtil {

    private static final String COMMON_DIR = "common";

    private static final String READER_SUFFIX = "reader";

    private static final String WRITER_SUFFIX = "writer";

    private static final String PACKAGE_PREFIX = "com.dtstack.flinkx.";

    public static Set<URL> getJarFileDirPath(String pluginName, String pluginRoot){
        Set<URL> urlList = new HashSet<>();

        File commonDir = new File(pluginRoot + File.separator + COMMON_DIR + File.separator);
        File pluginDir = new File(pluginRoot + File.separator + pluginName);

        try {
            urlList.addAll(SysUtil.findJarsInDir(commonDir));
            urlList.addAll(SysUtil.findJarsInDir(pluginDir));

            return urlList;
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getPluginClassName(String pluginName){
        String pluginClassName;
        if(pluginName.toLowerCase().endsWith(READER_SUFFIX)) {
            pluginClassName = PACKAGE_PREFIX + camelize(pluginName, READER_SUFFIX);
        } else if(pluginName.toLowerCase().endsWith(WRITER_SUFFIX)) {
            pluginClassName = PACKAGE_PREFIX + camelize(pluginName, WRITER_SUFFIX);
        } else {
            throw new IllegalArgumentException("Plugin Name should end with reader, writer or database");
        }

        return pluginClassName;
    }

    private static String camelize(String pluginName, String suffix) {
        int pos = pluginName.indexOf(suffix);
        String left = pluginName.substring(0, pos);
        left = left.toLowerCase();
        suffix = suffix.toLowerCase();
        StringBuffer sb = new StringBuffer();
        sb.append(left + "." + suffix + ".");
        sb.append(left.substring(0,1).toUpperCase() + left.substring(1));
        sb.append(suffix.substring(0,1).toUpperCase() + suffix.substring(1));
        return sb.toString();
    }
}
