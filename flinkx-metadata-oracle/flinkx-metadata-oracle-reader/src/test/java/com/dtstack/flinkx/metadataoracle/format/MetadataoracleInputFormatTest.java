package com.dtstack.flinkx.metadataoracle.format;

import com.dtstack.flinkx.metadataoracle.inputformat.MetadataoracleInputFormat;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

public class MetadataoracleInputFormatTest {

    MetadataoracleInputFormat metadataoracleInputFormat;
    private Connection c;
    private Statement stmt;
    private ResultSet rs;

    @Before
    public void setup(){
        metadataoracleInputFormat = new MetadataoracleInputFormat();
        c = mock(Connection.class);
        stmt = mock(Statement.class);
        rs = mock(ResultSet.class);

    }

    @Test
    public void queryTables() throws Exception {
        Whitebox.setInternalState(metadataoracleInputFormat, "connection", c);
        Whitebox.setInternalState(metadataoracleInputFormat, "statement", stmt);
        when(c.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(any(String.class))).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        List<Object> objectList = metadataoracleInputFormat.showTables();
        assertEquals(objectList.size(),0);

    }
}
