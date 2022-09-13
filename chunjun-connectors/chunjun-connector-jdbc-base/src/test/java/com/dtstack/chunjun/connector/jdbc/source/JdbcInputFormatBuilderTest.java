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

package com.dtstack.chunjun.connector.jdbc.source;

import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/** @author liuliu 2022/8/15 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JdbcInputFormat.class, JdbcConf.class, JdbcDialect.class})
public class JdbcInputFormatBuilderTest {

    private static JdbcInputFormatBuilder jdbcInputFormatBuilder;
    private static JdbcConf jdbcConf;

    @BeforeClass
    public static void setup() {
        JdbcInputFormat jdbcInputFormat = mock(JdbcInputFormat.class);
        jdbcConf = mock(JdbcConf.class);
        jdbcInputFormatBuilder = new JdbcInputFormatBuilder(jdbcInputFormat);

        when(jdbcInputFormat.getJdbcConf()).thenReturn(jdbcConf);
        when(jdbcConf.getParallelism()).thenReturn(3);
    }

    @Test
    public void checkFormatTest() {
        // startLocation error
        when(jdbcConf.getStartLocation()).thenReturn("10,11");

        // semantic error
        when(jdbcConf.getSemantic()).thenReturn("asd");
        Exception e = null;
        try {
            jdbcInputFormatBuilder.checkFormat();
        } catch (Exception exception) {
            e = exception;
        }
        Assert.assertNotNull(e);
    }
}
