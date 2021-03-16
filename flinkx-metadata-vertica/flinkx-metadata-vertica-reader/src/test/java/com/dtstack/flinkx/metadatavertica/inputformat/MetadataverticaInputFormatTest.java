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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

public class MetadataverticaInputFormatTest {


    private MetadataverticaInputFormat inputFormat;
    private Connection c;
    private Statement stmt;
    private ResultSet rs;
    private ResultSetMetaData rd;
    private DatabaseMetaData dm;
    private Object currentObject;

    @Before
    public void setup(){
        inputFormat = new MetadataverticaInputFormat();
        c = mock(Connection.class);
        stmt = mock(Statement.class);
        rs = mock(ResultSet.class);
        dm = mock(DatabaseMetaData.class);
        rd = mock(ResultSetMetaData.class);

    }




    @Test
    public void testShowTable() throws SQLException{
        Whitebox.setInternalState(inputFormat, "connection", c);
        Whitebox.setInternalState(inputFormat, "statement", stmt);
        when(c.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(any(String.class))).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        when(rs.getMetaData()).thenReturn(rd);
        when(c.getMetaData()).thenReturn(dm);
        when(dm.getTables(null,null,null,null)).thenReturn(rs);
        List<Object> objectList = inputFormat.showTables();
        assertEquals(objectList.size(),0);
    }
    
}
