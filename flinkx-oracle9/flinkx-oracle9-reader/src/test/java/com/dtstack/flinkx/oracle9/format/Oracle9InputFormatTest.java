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

package com.dtstack.flinkx.oracle9.format;

import com.dtstack.flinkx.util.ReflectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import sun.misc.URLClassPath;

import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/5/11 13:50
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ReflectionUtils.class,FilenameUtils.class,FlinkUserCodeClassLoaders.class})
public class Oracle9InputFormatTest {


    private static final String xml = "<label>This is an XML fragment</label>";

    @Test
    public void getConnectionTest() throws Exception {
        Oracle9InputFormat inputFormat = PowerMockito.mock(Oracle9InputFormat.class);
        Field declaredField = PowerMockito.mock(Field.class);
        URLClassPath urlClassPath = PowerMockito.mock(URLClassPath.class);
        URL[] url = new URL[1];
        URLClassLoader urlClassLoader = PowerMockito.mock(URLClassLoader.class);
        PowerMockito.mockStatic(ReflectionUtils.class);
        PowerMockito.mockStatic(FilenameUtils.class);
        PowerMockito.mockStatic(FlinkUserCodeClassLoaders.class);
        url[0] = new URL("file://");
        PowerMockito.doCallRealMethod().when(inputFormat).getConnection();
        PowerMockito.when(ReflectionUtils.getDeclaredField(any(), anyString())).thenReturn(declaredField);
        PowerMockito.when(declaredField.get(any())).thenReturn(urlClassPath);
        PowerMockito.when(urlClassPath.getURLs()).thenReturn(url);
        PowerMockito.when(FilenameUtils.getName(any())).thenReturn("flinkx-oracle9-reader-1.10_release_4.1.x.jar");
        PowerMockito.when(FlinkUserCodeClassLoaders.childFirst(any(), any(), any())).thenReturn(urlClassLoader);
        PowerMockito.field(Oracle9InputFormat.class, "driverName").set(inputFormat, "oracle.jdbc.driver.OracleDriver");
    }

}
