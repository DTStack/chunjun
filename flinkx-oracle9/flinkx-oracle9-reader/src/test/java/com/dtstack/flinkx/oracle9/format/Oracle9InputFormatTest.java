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

import com.dtstack.flinkx.classloader.PluginUtil;
import com.dtstack.flinkx.oracle9.IOracle9Helper;
import com.dtstack.flinkx.oracle9.OracleUtil;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.GsonUtil;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.LoggerFactory;

import java.net.URLClassLoader;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/5/11 13:50
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({PluginUtil.class, ClassUtil.class, OracleUtil.class, GsonUtil.class, FlinkUserCodeClassLoaders.class})
public class Oracle9InputFormatTest {


    @Test
    public void getConnectionTest() throws Exception {
        Oracle9InputFormat inputFormat = PowerMockito.mock(Oracle9InputFormat.class);
        URLClassLoader urlClassLoader = PowerMockito.mock(URLClassLoader.class);
        IOracle9Helper helper = PowerMockito.mock(IOracle9Helper.class);
        Connection connection = PowerMockito.mock(Connection.class);

        PowerMockito.field(Oracle9InputFormat.class, "needLoadJarPath").set(inputFormat, "D://");
        PowerMockito.field(Oracle9InputFormat.class,"LOG").set(inputFormat, LoggerFactory.getLogger(getClass()));

        PowerMockito.mockStatic(PluginUtil.class);
        PowerMockito.mockStatic(ClassUtil.class);
        PowerMockito.mockStatic(OracleUtil.class);
        PowerMockito.mockStatic(GsonUtil.class);
        PowerMockito.mockStatic(FlinkUserCodeClassLoaders.class);
        List<String> list = new ArrayList<>();
        list.add("ojdbc6-11.2.0.4.jar");
        list.add("xdb6-11.2.0.4.jar");
        list.add("xmlparserv2-11.2.0.4.jar");
//        PowerMockito.suppress(PowerMockito.method(Oracle9InputFormat.class, "open", JdbcInputSplit.class));

        PowerMockito.when(PluginUtil.getAllJarNames(any())).thenReturn(list);
        PowerMockito.when(FlinkUserCodeClassLoaders.childFirst(any(), any(), any())).thenReturn(urlClassLoader);
        PowerMockito.when(OracleUtil.getOracleHelper(any())).thenReturn(helper);

        PowerMockito.doNothing().when(ClassUtil.class);
        ClassUtil.forName(any(), any());

        PowerMockito.when(helper.getConnection(any(), any(), any())).thenReturn(connection);
        PowerMockito.doCallRealMethod().when(inputFormat).getConnection();
        inputFormat.getConnection();
    }

}
