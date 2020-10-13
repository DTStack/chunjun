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

package com.dtstack.flinkx.util;

import com.dtstack.flinkx.classloader.PluginUtil;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.Set;

/**
 * @author tiezhu
 * Date 2020/6/19 星期五
 */
public class PluginUtilTest {

    @Test
    public void testGetJarFileDirPath() {
        String pluginName = "mysqlreader";
        String pluginRoot = "F:\\dtstack_workplace\\project_workplace\\flinkx\\code\\flinkx\\syncplugins";
        String remotePluginPath = "F:\\dtstack_workplace\\project_workplace\\flinkx\\code\\flinkx\\syncplugins";

        Assert.assertEquals(4, PluginUtil.getJarFileDirPath(pluginName, pluginRoot, remotePluginPath).size());
    }
}
