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

package com.dtstack.flinkx.postgresql.writer;

import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.postgresql.format.PostgresqlOutputFormat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.DriverManager;
import java.util.HashMap;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DriverManager.class, PostgresqlOutputFormat.class})
public class PostgresqlOutputFormatBuilderTest {
    private PostgresqlOutputFormatBuilder builder;


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        builder = new PostgresqlOutputFormatBuilder(new PostgresqlOutputFormat());
    }


    @Test
    public void testCheckFormat() {
        builder.setDbUrl("jdbc:postgresql://localhost:5432/daishu_test?currentSchema=public");
        builder.setUsername("root");
        builder.setPassword("ps");
        builder.setDriverName("org.postgresql.Driver");
        builder.setBatchInterval(10);
        HashMap<String, Object> map = new HashMap<>();
        map.put("isRestore", true);
        RestoreConfig restoreConfig = new RestoreConfig(map);
        builder.setRestoreConfig(restoreConfig);

        builder.setSourceType("TEST");
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("sourceType must be one of [POSTGRESQL, ADB], current sourceType is [TEST]");
        builder.checkFormat();
    }


}
