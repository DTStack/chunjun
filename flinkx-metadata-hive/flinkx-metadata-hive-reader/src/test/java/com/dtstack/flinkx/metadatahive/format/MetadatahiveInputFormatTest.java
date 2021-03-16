package com.dtstack.flinkx.metadatahive.format;

import com.dtstack.flinkx.metadatahive.inputformat.MetadatahiveInputFormat;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

public class MetadatahiveInputFormatTest {

    MetadatahiveInputFormat metadatahiveInputFormat;
    private Connection c;
    private Statement stmt;
    private ResultSet rs;
    private ResultSetMetaData rd;

    @Before
    public void setup(){
        metadatahiveInputFormat = new MetadatahiveInputFormat();
        c = mock(Connection.class);
        stmt = mock(Statement.class);
        rs = mock(ResultSet.class);
        rd = mock(ResultSetMetaData.class);

    }

    @Test
    public void queryTables() throws Exception {
        Whitebox.setInternalState(metadatahiveInputFormat, "connection", c);
        Whitebox.setInternalState(metadatahiveInputFormat, "statement", stmt);
        when(c.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(any(String.class))).thenReturn(rs);
        when(rs.getMetaData()).thenReturn(rd);
        when(rs.next()).thenReturn(false);
        when(rd.getColumnCount()).thenReturn(1);
        List<Object> objectList = metadatahiveInputFormat.showTables();
        assertEquals(objectList.size(),0);

    }
}
