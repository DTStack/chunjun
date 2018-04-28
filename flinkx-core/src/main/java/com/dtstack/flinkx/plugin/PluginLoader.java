/**
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

package com.dtstack.flinkx.plugin;

import com.dtstack.flinkx.loader.DTClassLoader;
import com.dtstack.flinkx.util.SysUtil;
import org.apache.flink.util.Preconditions;
import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * FlinkX Plguin Loader
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class PluginLoader{

    private String pluginRoot;

    private String pluginName;

    private String pluginClassName;

    private List<URL> urlList = new ArrayList<>();

    private final String pkgPrefix = "com.dtstack.flinkx.";

    private final String COMMON_DIR = "common";

    private final String READER_SUFFIX = "reader";

    private final String WRITER_SUFFIX = "writer";

    public PluginLoader(String pluginName, String pluginRoot) {

        Preconditions.checkArgument(pluginName != null && pluginName.trim().length() != 0);
        Preconditions.checkArgument(pluginRoot != null);

        this.pluginName = pluginName;
        this.pluginRoot = pluginRoot;
        String lowerPluginName = pluginName.toLowerCase();

        if(lowerPluginName.endsWith(READER_SUFFIX)) {
            pluginClassName = pkgPrefix + camelize(pluginName, READER_SUFFIX);
        } else if(lowerPluginName.endsWith(WRITER_SUFFIX)) {
            pluginClassName = pkgPrefix + camelize(pluginName, WRITER_SUFFIX);
        } else {
            throw new IllegalArgumentException("Plugin Name should end with reader, writer or database");
        }

    }

    private String camelize(String pluginName, String suffix) {
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

    public Class<?> getPluginClass() {
        File commonDir = new File(pluginRoot + File.separator + COMMON_DIR + File.separator);
        File pluginDir = new File(pluginRoot + File.separator + pluginName);

        try {
            urlList.addAll(SysUtil.findJarsInDir(commonDir));
            urlList.addAll(SysUtil.findJarsInDir(pluginDir));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }


        URL[] urls = urlList.toArray(new URL[urlList.size()]);
        DTClassLoader classLoader = new DTClassLoader(urls);
        Class<?> clazz = null;

        try {
            clazz = classLoader.loadClass(this.pluginClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        return clazz;
    }

}
