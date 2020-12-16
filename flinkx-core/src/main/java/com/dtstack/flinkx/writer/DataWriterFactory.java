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

package com.dtstack.flinkx.writer;

import com.dtstack.flinkx.classloader.ClassLoaderManager;
import com.dtstack.flinkx.classloader.PluginUtil;
import com.dtstack.flinkx.config.DataTransferConfig;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Set;

/**
 * The factory of Writer plugins
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class DataWriterFactory {

    private DataWriterFactory() {}

    public static BaseDataWriter getDataWriter(DataTransferConfig config) {
        try {
            String pluginName = config.getJob().getContent().get(0).getWriter().getName();
            String pluginClassName = PluginUtil.getPluginClassName(pluginName);
            Set<URL> urlList = PluginUtil.getJarFileDirPath(pluginName, config.getPluginRoot(), null);

            return ClassLoaderManager.newInstance(urlList, cl -> {
                Class<?> clazz = cl.loadClass(pluginClassName);
                Constructor constructor = clazz.getConstructor(DataTransferConfig.class);
                return (BaseDataWriter)constructor.newInstance(config);
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
