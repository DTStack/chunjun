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
package com.dtstack.chunjun.client.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Optional;

public class PluginInfoUtil {

    private static final String MAIN_CLASS = "com.dtstack.chunjun.Main";
    private static final String CORE_JAR_NAME_PREFIX = "chunjun";

    public static String getCoreJarPath(String pluginRoot) throws FileNotFoundException {
        return pluginRoot
                + File.separator
                + getCoreJarName(pluginRoot)
                        .orElseThrow(
                                () ->
                                        new FileNotFoundException(
                                                "Can not find core jar file in path:"
                                                        + pluginRoot));
    }

    public static Optional<String> getCoreJarName(String pluginRoot) {
        File pluginDir = new File(pluginRoot);
        if (pluginDir.exists() && pluginDir.isDirectory()) {
            File[] jarFiles =
                    pluginDir.listFiles(
                            (dir, name) ->
                                    name.toLowerCase().startsWith(CORE_JAR_NAME_PREFIX)
                                            && name.toLowerCase().endsWith(".jar"));

            if (jarFiles != null && jarFiles.length > 0) {
                return Optional.of(jarFiles[0].getName());
            }
        }
        return Optional.empty();
    }

    public static String getMainClass() {
        return MAIN_CLASS;
    }
}
