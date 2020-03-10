package com.dtstack.flinkx.metadata.reader.inputformat;

import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.metadata.MetaDataCons;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import com.dtstack.flinkx.metadata.util.ConnUtil;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
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

    protected String currentQueryTable;

    protected List<String> tableList;

    protected Map<String, Object> currentMessage;

    protected boolean hasNext;

    protected String driverName;

    protected Connection connection;
    protected Statement statement;

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("inputSplit = {}", inputSplit);
        currentMessage = new HashMap<>();
        currentQueryTable = ((MetaDataInputSplit) inputSplit).getTable();
        beforeUnit(currentQueryTable);
        currentMessage.put("data", unitMetaData(currentQueryTable));
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
            ConnUtil.closeConn(null, statement, connection, true);
        } catch (Exception e) {
            setErrorMessage(e, "shut down resources error!");
        }
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        if (table.isEmpty()) {
            tableList = getTableList();
        } else {
            tableList = table;
        }
        minNumSplits = tableList.size();
        InputSplit[] inputSplits = new MetaDataInputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new MetaDataInputSplit(i, numPartitions, dbUrl, tableList.get(i));
        }
        return inputSplits;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        row = new Row(1);
        if (errorMessage.isEmpty()) {
            currentMessage.put("querySuccess", true);
            currentMessage.put("errorMsg", errorMessage);
        } else {
            currentMessage.put("querySuccess", false);
            currentMessage.put("errorMsg", errorMessage);
        }
        row.setField(0, objectMapper.writeValueAsString(currentMessage));
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
            errorMessage.put("errorMessage", message + " detail:" + e.getMessage());
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
     * 组合元数据信息之前的操作，比如hive2的获取字段名和分区字段
     */
    protected abstract void beforeUnit(String currentQueryTable);

    /**
     * 从结果集中解析有关表的元数据信息
     *
     * @param currentQueryTable
     * @return 有关表的元数据信息
     */
    public abstract Map<String, Object> getTablePropertites(String currentQueryTable);

    /**
     * 从结果集中解析有关表字段的元数据信息
     *
     * @return 有关表字段的元数据信息
     */
    public abstract Map<String, Object> getColumnPropertites(String currentQueryTable);

    /**
     * 从结果集中解析有关分区字段的元数据信息
     *
     * @return 有关分区字段的元数据信息
     */
    public abstract Map<String, Object> getPartitionPropertites(String currentQueryTable);

    /**
     * 对元数据信息整合
     */
    public Map<String, Object> unitMetaData(String currentQueryTable) {
        Map<String, Object> tablePropertities = getTablePropertites(currentQueryTable);
        Map<String, Object> columnPreportities = getColumnPropertites(currentQueryTable);
        Map<String, Object> partitionPreprotities = getPartitionPropertites(currentQueryTable);
        Map<String, Object> result = new HashMap<>();
        result.put("dbTypeAndVersion", "");
        result.put(MetaDataCons.KEY_TABLE, currentQueryTable);
        result.put(MetaDataCons.KEY_OPERA_TYPE, "createTable");

        if (!columnPreportities.isEmpty()) {
            result.put(MetaDataCons.KEY_COLUMN, columnPreportities);
        }

        if (!tablePropertities.isEmpty()) {
            result.put(MetaDataCons.KEY_TABLE_PROPERTITES, tablePropertities);
        }

        if (!partitionPreprotities.isEmpty()) {
            result.put(MetaDataCons.KEY_PARTITION_PROPERTITES, partitionPreprotities);
        }

        return result;
    }

    /**
     * 如果传递的表为空，那么通过show tables 获取tableList
     */
    public List<String> getTableList() throws SQLException, ClassNotFoundException {
        List<String> tableList = new ArrayList<>();
        Class.forName(driverName);
        connection = DriverManager.getConnection(dbUrl, username, password);
        statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW tables");
        while (resultSet.next()) {
            tableList.add(resultSet.getString(1));
        }
        return tableList;
    }
}
