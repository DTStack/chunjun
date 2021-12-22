package com.dtstack.flinkx.connector.jdbc.sink;

import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.constants.CDCConstantValue;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.element.ColumnRowData;

import org.apache.flink.table.data.RowData;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 构建prepare代理 Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2021-12-20
 */
public class PreparedStmtProxy implements FieldNamedPreparedStatement {

    private static final Logger LOG = LoggerFactory.getLogger(PreparedStmtProxy.class);

    // LUR  database_table_rowkind
    protected Cache<String, DynamicPreparedStmt> pstmtCache;

    // 初始化的时候确定的类型
    protected transient FieldNamedPreparedStatement currentFieldNamedPstmt;

    /** 数据类型转换器 */
    protected AbstractRowConverter currentRowConverter;

    protected Connection connection;
    protected JdbcDialect jdbcDialect;
    protected boolean writeExtInfo;

    public PreparedStmtProxy(Connection connection, JdbcDialect jdbcDialect, boolean writeExtInfo) {
        this.connection = connection;
        this.jdbcDialect = jdbcDialect;
        this.writeExtInfo = writeExtInfo;

        pstmtCache =
                CacheBuilder.newBuilder()
                        .maximumSize(100)
                        .expireAfterAccess(60, TimeUnit.MINUTES)
                        .removalListener(
                                (RemovalListener<String, DynamicPreparedStmt>)
                                        notification -> {
                                            try {
                                                notification.getValue().close();
                                            } catch (SQLException e) {
                                                e.printStackTrace();
                                            }
                                        })
                        .build();
    }

    public PreparedStmtProxy(
            FieldNamedPreparedStatement currentFieldNamedPstmt,
            AbstractRowConverter currentRowConverter) {
        this.currentFieldNamedPstmt = currentFieldNamedPstmt;
        this.currentRowConverter = currentRowConverter;
    }

    public void convertToExternal(RowData row) throws Exception {
        getOrCreateFieldNamedPstmt(row);
        currentFieldNamedPstmt =
                (FieldNamedPreparedStatement)
                        currentRowConverter.toExternal(row, this.currentFieldNamedPstmt);
    }

    public void getOrCreateFieldNamedPstmt(RowData row) throws ExecutionException {
        if (row instanceof ColumnRowData) {
            ColumnRowData columnRowData = (ColumnRowData) row;
            Map<String, Integer> head = columnRowData.getHeaderInfo();
            int dataBaseIndex = head.get(CDCConstantValue.SCHEMA);
            int tableIndex = head.get(CDCConstantValue.TABLE);

            // jdbcDialect 提供统一的处理放肆 比如pg 表都是小写的
            String dataBase =
                    jdbcDialect.getDialectTableName(row.getString(dataBaseIndex).toString());
            String tableName =
                    jdbcDialect.getDialectTableName(
                            row.getString(tableIndex).toString().toLowerCase());
            String key = dataBase + "_" + tableName + "_" + row.getRowKind().toString();
            List<String> types = new ArrayList<>();

            DynamicPreparedStmt fieldNamedPreparedStatement =
                    pstmtCache.get(
                            key,
                            () -> {
                                try {
                                    return DynamicPreparedStmt.buildStmt(
                                            columnRowData.getHeaderInfo(),
                                            columnRowData.getExtHeader(),
                                            dataBase,
                                            tableName,
                                            columnRowData.getRowKind(),
                                            types,
                                            connection,
                                            jdbcDialect,
                                            writeExtInfo);
                                } catch (SQLException e) {
                                    LOG.warn("", e);
                                    return null;
                                }
                            });

            currentFieldNamedPstmt = fieldNamedPreparedStatement.getFieldNamedPreparedStatement();
            currentRowConverter = fieldNamedPreparedStatement.getRowConverter();
            if (!writeExtInfo) {
                columnRowData.removeExtHeaderInfo();
            }
        }
    }

    protected void writeSingleRecordInternal(RowData row) throws Exception {
        getOrCreateFieldNamedPstmt(row);
        currentFieldNamedPstmt =
                (FieldNamedPreparedStatement)
                        currentRowConverter.toExternal(row, this.currentFieldNamedPstmt);
        currentFieldNamedPstmt.execute();
    }

    @Override
    public void clearParameters() throws SQLException {
        currentFieldNamedPstmt.clearParameters();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return currentFieldNamedPstmt.executeQuery();
    }

    @Override
    public void addBatch() throws SQLException {
        currentFieldNamedPstmt.executeBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return currentFieldNamedPstmt.executeBatch();
    }

    @Override
    public void clearBatch() throws SQLException {
        currentFieldNamedPstmt.clearBatch();
    }

    @Override
    public boolean execute() throws SQLException {
        return currentFieldNamedPstmt.execute();
    }

    @Override
    public void setNull(int fieldIndex, int sqlType) throws SQLException {
        currentFieldNamedPstmt.setNull(fieldIndex, sqlType);
    }

    @Override
    public void setBoolean(int fieldIndex, boolean x) throws SQLException {
        currentFieldNamedPstmt.setBoolean(fieldIndex, x);
    }

    @Override
    public void setByte(int fieldIndex, byte x) throws SQLException {
        currentFieldNamedPstmt.setByte(fieldIndex, x);
    }

    @Override
    public void setShort(int fieldIndex, short x) throws SQLException {
        currentFieldNamedPstmt.setShort(fieldIndex, x);
    }

    @Override
    public void setInt(int fieldIndex, int x) throws SQLException {
        currentFieldNamedPstmt.setInt(fieldIndex, x);
    }

    @Override
    public void setLong(int fieldIndex, long x) throws SQLException {
        currentFieldNamedPstmt.setLong(fieldIndex, x);
    }

    @Override
    public void setFloat(int fieldIndex, float x) throws SQLException {
        currentFieldNamedPstmt.setFloat(fieldIndex, x);
    }

    @Override
    public void setDouble(int fieldIndex, double x) throws SQLException {
        currentFieldNamedPstmt.setDouble(fieldIndex, x);
    }

    @Override
    public void setBigDecimal(int fieldIndex, BigDecimal x) throws SQLException {
        currentFieldNamedPstmt.setBigDecimal(fieldIndex, x);
    }

    @Override
    public void setString(int fieldIndex, String x) throws SQLException {
        currentFieldNamedPstmt.setString(fieldIndex, x);
    }

    @Override
    public void setBytes(int fieldIndex, byte[] x) throws SQLException {
        currentFieldNamedPstmt.setBytes(fieldIndex, x);
    }

    @Override
    public void setDate(int fieldIndex, Date x) throws SQLException {
        currentFieldNamedPstmt.setDate(fieldIndex, x);
    }

    @Override
    public void setTime(int fieldIndex, Time x) throws SQLException {
        currentFieldNamedPstmt.setTime(fieldIndex, x);
    }

    @Override
    public void setTimestamp(int fieldIndex, Timestamp x) throws SQLException {
        currentFieldNamedPstmt.setTimestamp(fieldIndex, x);
    }

    @Override
    public void setObject(int fieldIndex, Object x) throws SQLException {
        currentFieldNamedPstmt.setObject(fieldIndex, x);
    }

    @Override
    public void setBlob(int fieldIndex, InputStream is) throws SQLException {
        currentFieldNamedPstmt.setBlob(fieldIndex, is);
    }

    @Override
    public void setClob(int fieldIndex, Reader reader) throws SQLException {
        currentFieldNamedPstmt.setClob(fieldIndex, reader);
    }

    @Override
    public void close() throws SQLException {
        currentFieldNamedPstmt.close();
    }
}
