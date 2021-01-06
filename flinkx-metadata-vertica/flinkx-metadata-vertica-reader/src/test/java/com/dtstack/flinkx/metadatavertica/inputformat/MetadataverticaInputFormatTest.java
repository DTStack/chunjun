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

package com.dtstack.flinkx.metadatavertica.inputformat;

import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MetadataverticaInputFormatTest {


    private MetadataverticaInputFormat inputFormat;

    @Before
    public void beforeMethod() throws NoSuchFieldException, IllegalAccessException, SQLException {
        inputFormat = new MetadataverticaInputFormat();
        inputFormat.switchDatabase("testDb");
        ThreadLocal<Connection> connectionTL = Mockito.mock(ThreadLocal.class);
        Connection connection = Mockito.mock(Connection.class);
        DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(connection.getMetaData()).thenReturn(metaData);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);
        Mockito.when(resultSet.getString(Mockito.anyString())).thenReturn("test");
        Mockito.when(metaData.getTables(null, "testDb", null, null)).thenReturn(resultSet);
        Mockito.when(connectionTL.get()).thenReturn(connection);
        Field connectionField = BaseMetadataInputFormat.class.getDeclaredField("connection");
        connectionField.setAccessible(true);
        connectionField.set(inputFormat, connectionTL);
    }


    @Test
    public void testShowTable() {
        Assert.assertEquals(inputFormat.showTables().size(), 1);
    }

    @Test
    public void testQuote() {
        Assert.assertEquals(inputFormat.quote("test"), "test");
    }
}
