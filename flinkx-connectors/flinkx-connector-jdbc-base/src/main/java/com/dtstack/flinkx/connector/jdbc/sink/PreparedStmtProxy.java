package com.dtstack.flinkx.connector.jdbc.sink;

import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.constants.CDCConstantValue;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.element.ColumnRowData;

import org.apache.flink.table.data.RowData;

import com.esotericsoftware.minlog.Log;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.apache.commons.collections.MapUtils;
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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * build prepare proxy, proxy implements FieldNamedPreparedStatement. it support to build
 * preparestmt and manager it with cache.
 *
 * <p></>Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2021-12-20
 */
public class PreparedStmtProxy implements FieldNamedPreparedStatement {

    private static final Logger LOG = LoggerFactory.getLogger(PreparedStmtProxy.class);

    private int cacheSize = 100;

    private int cacheDurationMin = 10;

    /** LUR cache key info: database_table_rowkind * */
    protected Cache<String, DynamicPreparedStmt> pstmtCache;

    /** 当前的执行sql的preparestatement */
    protected transient FieldNamedPreparedStatement currentFieldNamedPstmt;

    /** 当前执行sql的数据类型转换器 */
    protected AbstractRowConverter currentRowConverter;

    /** 当调用writeMultipleRecords 可能会涉及到多个pstmt */
    private final Set<FieldNamedPreparedStatement> unExecutePstmt = new LinkedHashSet<>();

    protected Connection connection;
    protected JdbcDialect jdbcDialect;

    /** 是否将框架额外添加的扩展信息写入到数据库,默认不写入* */
    protected boolean writeExtInfo;

    public PreparedStmtProxy(Connection connection, JdbcDialect jdbcDialect, boolean writeExtInfo) {
        this.connection = connection;
        this.jdbcDialect = jdbcDialect;
        this.writeExtInfo = writeExtInfo;

        pstmtCache =
                CacheBuilder.newBuilder()
                        .maximumSize(cacheSize)
                        .expireAfterAccess(cacheDurationMin, TimeUnit.MINUTES)
                        .removalListener(
                                (RemovalListener<String, DynamicPreparedStmt>)
                                        notification -> {
                                            try {
                                                assert notification.getValue() != null;
                                                notification.getValue().close();
                                            } catch (SQLException e) {
                                                Log.error("", e);
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
            if (MapUtils.isEmpty(head)) {
                return;
            }
            int dataBaseIndex = head.get(CDCConstantValue.SCHEMA);
            int tableIndex = head.get(CDCConstantValue.TABLE);

            // jdbcDialect 提供统一的处理 比如pg 表都是小写的
            String dataBase =
                    jdbcDialect.getDialectTableName(row.getString(dataBaseIndex).toString());
            String tableName =
                    jdbcDialect.getDialectTableName(
                            row.getString(tableIndex).toString().toLowerCase());
            String key = dataBase + "_" + tableName + "_" + row.getRowKind().toString();

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
        currentFieldNamedPstmt.addBatch();
        unExecutePstmt.add(currentFieldNamedPstmt);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        List<Integer> exeResult = new ArrayList<>();
        for (FieldNamedPreparedStatement pstmt : unExecutePstmt) {
            int[] resultArray = pstmt.executeBatch();
            Arrays.stream(resultArray).forEach(exeResult::add);
        }

        int[] result = new int[exeResult.size()];
        for (int i = 0; i < exeResult.size(); i++) {
            result[i] = exeResult.get(i);
        }
        return result;
    }

    @Override
    public void clearBatch() throws SQLException {
        for (FieldNamedPreparedStatement pstmt : unExecutePstmt) {
            pstmt.clearBatch();
        }
        unExecutePstmt.clear();
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
