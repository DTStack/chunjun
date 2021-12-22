package com.dtstack.flinkx.connector.jdbc.sink;

import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatementImpl;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2021-12-20
 */
public class DynamicPreparedStmt {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicPreparedStmt.class);

    protected List<String> columnNameList = new ArrayList<>();

    protected List<String> columnTypeList = new ArrayList<>();

    protected transient FieldNamedPreparedStatement fieldNamedPreparedStatement;
    protected JdbcConf jdbcConf;
    private String key;
    private boolean writeExtInfo;
    private RowKind rowKind;
    private JdbcDialect jdbcDialect;
    private AbstractRowConverter rowConverter;

    public static DynamicPreparedStmt buildStmt(
            Map<String, Integer> header,
            Set<String> extHeader,
            String schemaName,
            String tableName,
            RowKind rowKind,
            List<String> types,
            Connection connection,
            JdbcDialect jdbcDialect,
            boolean writeExtInfo)
            throws SQLException {
        DynamicPreparedStmt dynamicPreparedStmt = new DynamicPreparedStmt();

        dynamicPreparedStmt.writeExtInfo = writeExtInfo;
        dynamicPreparedStmt.getColumnNameList(header, extHeader);
        dynamicPreparedStmt.getColumnMeta(schemaName, tableName, connection);
        dynamicPreparedStmt.jdbcDialect = jdbcDialect;
        dynamicPreparedStmt.columnTypeList = types;
        dynamicPreparedStmt.init();

        String sql = dynamicPreparedStmt.prepareTemplates(rowKind, schemaName, tableName);
        String[] fieldNames = new String[dynamicPreparedStmt.columnNameList.size()];
        dynamicPreparedStmt.columnNameList.toArray(fieldNames);
        dynamicPreparedStmt.fieldNamedPreparedStatement =
                FieldNamedPreparedStatementImpl.prepareStatement(connection, sql, fieldNames);
        return dynamicPreparedStmt;
    }

    protected String prepareTemplates(RowKind rowKind, String schemaName, String tableName) {
        String singleSql = null;
        switch (rowKind) {
            case INSERT:
                singleSql =
                        jdbcDialect.getInsertIntoStatement(
                                schemaName, tableName, columnNameList.toArray(new String[0]));
                break;
            case DELETE:
                String[] columnNames = new String[columnNameList.size()];
                columnNameList.toArray(columnNames);
                singleSql = jdbcDialect.getDeleteStatement(schemaName, tableName, columnNames);
                break;
            default:
                LOG.warn("not support rowkind " + rowKind.toString());
        }

        return singleSql;
    }

    public void getColumnNameList(Map<String, Integer> header, Set<String> extHeader) {
        if (writeExtInfo) {
            columnNameList.addAll(header.keySet());
        } else {
            header.keySet().stream()
                    .filter(tmpkey -> !extHeader.contains(tmpkey))
                    .forEach(tmpkey -> columnNameList.add(tmpkey));
        }
    }

    public void init() {
        RowType rowType =
                TableUtil.createRowType(
                        columnNameList, columnTypeList, jdbcDialect.getRawTypeConverter());
        rowConverter = jdbcDialect.getColumnConverter(rowType, jdbcConf);
    }

    public void getColumnMeta(String schema, String table, Connection dbConn) {
        Pair<List<String>, List<String>> listListPair =
                JdbcUtil.getTableMetaData(schema, table, dbConn);
        List<String> nameList = listListPair.getLeft();
        List<String> typeList = listListPair.getRight();
        for (String columnName : columnNameList) {
            int index = nameList.indexOf(columnName);
            columnTypeList.add(typeList.get(index));
        }
    }

    public void writeRow(RowData row) throws Exception {
        fieldNamedPreparedStatement =
                (FieldNamedPreparedStatement)
                        rowConverter.toExternal(row, this.fieldNamedPreparedStatement);
        fieldNamedPreparedStatement.execute();
    }

    public void close() throws SQLException {
        fieldNamedPreparedStatement.close();
    }

    public FieldNamedPreparedStatement getFieldNamedPreparedStatement() {
        return fieldNamedPreparedStatement;
    }

    public void setFieldNamedPreparedStatement(
            FieldNamedPreparedStatement fieldNamedPreparedStatement) {
        this.fieldNamedPreparedStatement = fieldNamedPreparedStatement;
    }

    public AbstractRowConverter getRowConverter() {
        return rowConverter;
    }

    public void setRowConverter(AbstractRowConverter rowConverter) {
        this.rowConverter = rowConverter;
    }
}
