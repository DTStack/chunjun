package com.dtstack.flinkx.metadatamysql.format;

import com.dtstack.flinkx.metadatamysql.inputformat.MetadatamysqlInputFormat;
import com.dtstack.metadata.rdb.core.entity.TableEntity;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_CREATE_TIME;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_DATA_LENGTH;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_ENGINE;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_ROWS;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_ROW_FORMAT;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_TABLE_COMMENT;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

public class MetadatamysqlInputFormatTest {

    MetadatamysqlInputFormat metadatamysqlInputFormat;
    private Connection c;
    private Statement stmt;
    private ResultSet rs;

    @Before
    public void setup(){
        metadatamysqlInputFormat = new MetadatamysqlInputFormat();
        c = mock(Connection.class);
        stmt = mock(Statement.class);
        rs = mock(ResultSet.class);

    }

    @Test
    public void createTableEntity() throws Exception {
        Whitebox.setInternalState(metadatamysqlInputFormat, "connection", c);
        Whitebox.setInternalState(metadatamysqlInputFormat, "statement", stmt);
        when(c.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(any(String.class))).thenReturn(rs);
        when(rs.getString(RESULT_TABLE_COMMENT)).thenReturn("test");
        when(rs.getString(RESULT_CREATE_TIME)).thenReturn("20200101");
        when(rs.getLong(RESULT_DATA_LENGTH)).thenReturn(1000L);
        when(rs.getLong(RESULT_ROWS)).thenReturn(1000L);
        when(rs.getString(RESULT_ENGINE)).thenReturn("binlog");
        when(rs.getString(RESULT_ROW_FORMAT)).thenReturn("test");
        when(rs.next()).thenReturn(false);
        TableEntity tableEntity = metadatamysqlInputFormat.createTableEntity();
        assertEquals(tableEntity.getComment(),null);

    }
}
