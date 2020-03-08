package com.dtstack.flinkx.metadata.reader.inputformat;

import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import com.dtstack.flinkx.metadata.util.ConnUtil;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/8
 * @description : 元数据读取的抽象类
 */
public abstract class MetaDataInputFormat extends RichInputFormat {
    protected int numPartitions;
    protected Map<String, String> errorMessage = new HashMap<>();
    protected String dbUrl;
    protected List<String> table;
    protected String username;
    protected String password;
    protected ResultSet resultSet;

    protected String currentQueryTable;

    protected Map<String, Object> currentMessage;

    protected boolean hasNext;

    protected String driverName;

    protected Connection connection;
    protected Statement statement;

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("inputSplit = {}", inputSplit);
        currentQueryTable = ((MetaDataInputSplit) inputSplit).getTable();
        resultSet = executeSql(buildQuerySql(currentQueryTable));
        currentMessage = getMetaData(resultSet);
        hasNext = true;

    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        try {
            Class.forName(driverName);
            connection = DriverManager.getConnection(dbUrl, username, password);
            statement = connection.createStatement();
        } catch (Exception e) {
            setErrorMessage(e, "connect error! current dbUrl" + dbUrl);
        }
    }

    @Override
    public void closeInputFormat() {
        try {
            super.closeInputFormat();
            ConnUtil.closeConn(resultSet, statement, connection, true);
        } catch (Exception e) {
            setErrorMessage(e, "shut down resources error!");
        }
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        minNumSplits = table.size();
        InputSplit[] inputSplits = new MetaDataInputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new MetaDataInputSplit(i, numPartitions, dbUrl, table.get(i));
        }
        return inputSplits;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        row = new Row(1);
        if (currentMessage.isEmpty()) {
            row.setField(0, objectMapper.writeValueAsString(errorMessage));
            errorMessage.clear();
        } else {
            row.setField(0, objectMapper.writeValueAsString(currentMessage));
        }
        hasNext = false;

        return row;
    }

    @Override
    protected void closeInternal() throws IOException {
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    /**
     * 任务异常信息
     */
    protected void setErrorMessage(Exception e, String message) {
        if (errorMessage.isEmpty()) {
            errorMessage.put("errorMessage", message);
            errorMessage.put(e.getClass().getSimpleName(), e.getMessage());
        }
    }

    /**
     * 执行查询计划
     */
    protected ResultSet executeSql(String sql) {
        ResultSet result = null;
        try {
            result = statement.executeQuery(sql);
        } catch (SQLException e) {
            setErrorMessage(e, "query error! current sql: " + sql);
        }
        return result;
    }

    /**
     * 查询结果集解析
     *
     * @param resultSet 查询结果集
     * @return Map格式的元数据信息
     */
    public abstract Map<String, Object> getMetaData(ResultSet resultSet);

    /**
     * 构建查询元数据sql
     *
     * @param currentQueryTable 当前查询的table
     * @return 查询语句sql
     */
    public abstract String buildQuerySql(String currentQueryTable);

}
