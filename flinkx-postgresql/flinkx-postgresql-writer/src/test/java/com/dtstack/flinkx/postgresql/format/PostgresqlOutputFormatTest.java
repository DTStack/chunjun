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

package com.dtstack.flinkx.postgresql.format;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DriverManager.class, PostgresqlOutputFormat.class})
public class PostgresqlOutputFormatTest {

    private Connection c;
    private DatabaseMetaData metaData;
    private PostgresqlOutputFormat postgresqlOutputFormat;
    @Before
    public void setup() throws SQLException {
        c = mock(Connection.class);
        metaData = mock(DatabaseMetaData.class);
        postgresqlOutputFormat = Mockito.mock(PostgresqlOutputFormat.class);

        mockStatic(PostgresqlOutputFormat.class);
        mockStatic(DriverManager.class);

        PowerMockito.when(DriverManager.getConnection(any(), any(), any())).thenReturn(c);
    }

    @Test
    public void testCheckFormatAdbVersion() throws SQLException {

        Whitebox.setInternalState(postgresqlOutputFormat, "mode", "update");
        Whitebox.setInternalState(postgresqlOutputFormat, "sourceType", "ADB");

        when(postgresqlOutputFormat.checkUpsert()).thenCallRealMethod();

        when(c.getMetaData()).thenReturn(metaData);
        when(metaData.getDatabaseProductVersion()).thenReturn("9.4").thenReturn("9.3");

        String s = postgresqlOutputFormat.checkUpsert();
        Assert.assertEquals(0, s.length());

        s = postgresqlOutputFormat.checkUpsert();
        Assert.assertTrue(s.length() > 0);
    }

    @Test
    public void testCheckFormatPostgreSqlVersion() throws SQLException {

        PostgresqlOutputFormat postgresqlOutputFormat = Mockito.mock(PostgresqlOutputFormat.class);
        Whitebox.setInternalState(postgresqlOutputFormat, "mode", "update");
        Whitebox.setInternalState(postgresqlOutputFormat, "sourceType", "POSTGRESQL");

        when(postgresqlOutputFormat.checkUpsert()).thenCallRealMethod();

        when(c.getMetaData()).thenReturn(metaData);
        when(metaData.getDatabaseProductVersion()).thenReturn("9.4").thenReturn("9.5.24");

        String s = postgresqlOutputFormat.checkUpsert();
        Assert.assertTrue(s.length() > 0);

        s = postgresqlOutputFormat.checkUpsert();
        Assert.assertEquals(0, s.length());
    }

}
