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
import static org.mockito.Mockito.doCallRealMethod;
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
        postgresqlOutputFormat = mock(PostgresqlOutputFormat.class);

        mockStatic(PostgresqlOutputFormat.class);
        mockStatic(DriverManager.class);

        PowerMockito.when(DriverManager.getConnection(any(), any(), any())).thenReturn(c);
    }

    @Test(expected = RuntimeException.class)
    public void testCheckFormatAdbVersion() throws SQLException {

        Whitebox.setInternalState(postgresqlOutputFormat, "mode", "update");
        Whitebox.setInternalState(postgresqlOutputFormat, "sourceType", "ADB");

        doCallRealMethod().when(postgresqlOutputFormat).checkUpsert();
        when(c.getMetaData()).thenReturn(metaData);

        when(metaData.getDatabaseProductVersion()).thenReturn("11.2");
        postgresqlOutputFormat.checkUpsert();

        when(metaData.getDatabaseProductVersion()).thenReturn("9.3");
        postgresqlOutputFormat.checkUpsert();
    }

    @Test(expected = RuntimeException.class)
    public void testCheckFormatPostgreSqlVersion() throws SQLException {

        PostgresqlOutputFormat postgresqlOutputFormat = Mockito.mock(PostgresqlOutputFormat.class);
        Whitebox.setInternalState(postgresqlOutputFormat, "mode", "update");
        Whitebox.setInternalState(postgresqlOutputFormat, "sourceType", "POSTGRESQL");

        doCallRealMethod().when(postgresqlOutputFormat).checkUpsert();

        when(c.getMetaData()).thenReturn(metaData);

        when(metaData.getDatabaseProductVersion()).thenReturn("10.2.34");
        postgresqlOutputFormat.checkUpsert();

        when(metaData.getDatabaseProductVersion()).thenReturn("9.4");
        postgresqlOutputFormat.checkUpsert();

    }

}
