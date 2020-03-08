package com.dtstack.flinkx.metadatasync.reader.inputformat;

import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.rdb.util.DBUtil;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/8
 * @description : 元数据读取子类
 */
public class AbstractMetaInputFormat extends RichInputFormat {
    protected int numPartitions;

    protected Map<String, String> errorMessage = new HashMap<>();
    protected String dbUrl;
    protected List<String> table;
    protected String username;
    protected String password;
    protected ResultSet resultSet;

    protected Map<String, Map<String, String>> filterData;

    protected List<String> tableColumn = new ArrayList<>();
    protected List<String> partitionColumn = new ArrayList<>();
    protected String currentQueryTable;

    protected String driverName = "org.apache.hive.jdbc.HiveDriver";
    protected Connection connection;
    protected Statement statement;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        try {
            Class.forName(driverName);
            connection = DriverManager.getConnection(dbUrl, username, password);
            statement = connection.createStatement();
        } catch (SQLException | ClassNotFoundException e) {
            setErrorMessage(e, "can not connect to hive! current dbUrl: " + dbUrl);
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        minNumSplits = table.size();
        InputSplit[] inputSplits = new MetadataInputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new MetadataInputSplit(i, numPartitions, dbUrl, table.get(i));
        }
        return inputSplits;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        return null;
    }

    @Override
    protected void closeInternal() throws IOException {

    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public void closeInputFormat() throws IOException {
        super.closeInputFormat();
        DBUtil.closeDBResources(resultSet, statement, connection, true);
    }

    /**
     * 去除无关数据
     */
    public void cleanData(Map<String, Map<String, String>> map) {
        for (Object obj : map.keySet().toArray()) {
            if (((String) obj).contains("#")) {
                map.remove(obj);
            }
        }
    }

    public void cleanData(List<String> list) {
        list.removeIf(item -> item.contains("#") || item.isEmpty());
    }

    /**
     * 生成任务异常信息map
     */
    public void setErrorMessage(Exception e, String message) {
        if (errorMessage.isEmpty()) {
            errorMessage.put("error message", message);
            errorMessage.put(e.getClass().getSimpleName(), e.getMessage());
        }
    }

    /**
     * 执行查询计划
     */
    public ResultSet executeSql(String sql) {
        ResultSet result = null;
        try {
            result = statement.executeQuery(sql);
        } catch (SQLException e) {
            setErrorMessage(e, "query error! current sql: " + sql);
        }
        return result;
    }

    // TODO 以下方法不同版本的hive查询语句可能不一样

    /**
     * 从查询结果中获取分区字段和非分区字段
     */
    public void getColumn() {
        try {
            boolean isPartitionColumn = false;
            ResultSet temp = executeSql(buildDescSql(false));
            while (temp.next()) {
                if (temp.getString(1).trim().contains("Partition Information")) {
                    isPartitionColumn = true;
                }
                if (isPartitionColumn) {
                    partitionColumn.add(temp.getString(1).trim());
                } else {
                    tableColumn.add(temp.getString(1));
                }
            }
            // 除去多余的字段
            cleanData(tableColumn);
            cleanData(partitionColumn);

        } catch (Exception e) {
            setErrorMessage(e, "get column error!");
        }
    }

    /**
     * 构建查询语句
     */
    public String buildDescSql(boolean formatted) {
        String sql;
        if (formatted) {
            sql = "DESC FORMATTED " + currentQueryTable;
        } else {
            sql = "DESC " + currentQueryTable;
        }
        return sql;
    }

    /**
     * 从查询结果中构建Map
     */
    public Map<String, Map<String, String>> transformDataToMap(ResultSet resultSet) {
        Map<String, Map<String, String>> result = new HashMap<>();
        String tempK = "";
        Map<String, String> map = new HashMap<>();
        try {
            while (resultSet.next()) {
                String tempK1 = resultSet.getString(1).replace(":", "").trim();
                String tempK2 = resultSet.getString(2);
                String tempVal = resultSet.getString(3);
                if (!tempK1.isEmpty()) {
                    if (!map.isEmpty()) {
                        result.put(tempK, map);
                        map = new HashMap<>();
                    }
                    tempK = tempK1;
                    map.put(tempK2, tempVal);
                } else {
                    if (tempK2 != null) {
                        map.put(tempK2.trim(), tempVal);
                    } else {
                        map.put(tempK2, tempVal);
                    }
                    continue;
                }
                result.put(tempK, map);
            }
            result.put(tempK, map);
            cleanData(result);
        } catch (Exception e) {
            setErrorMessage(e, "transform data error");
        }
        return result;
    }

    /**
     * 通过inputSplit获取元数据信息
     */
    public Map<String, Object> getMetaData(InputSplit inputSplit) {
        Map<String, Object> map = new HashMap<>();
        try {
            currentQueryTable = ((MetadataInputSplit) inputSplit).getTable();
            dbUrl = ((MetadataInputSplit) inputSplit).getDbUrl();
            resultSet = statement.executeQuery(buildDescSql(true));

            filterData = transformDataToMap(resultSet);
            getColumn();
            map = selectUsedData(filterData);
        } catch (Exception e) {
            setErrorMessage(e, "get Metadata error");
        }
        return map;
    }

    /**
     * 从查询的结果中构建需要的信息
     */
    public Map<String, Object> selectUsedData(Map<String, Map<String, String>> map) {
        Map<String, Object> result = new HashMap<>();
        Map<String, String> temp = new HashMap<>();
        List<Map> tempColumnList = new ArrayList<>();
        List<Map> tempPartitionColumnList = new ArrayList<>();

        for (String item : tableColumn) {
            tempColumnList.add(setColumnMap(item, map.get(item), tableColumn.indexOf(item)));
        }
        result.put(MetaDataCons.KEY_COLUMN, tempColumnList);

        for (String item : partitionColumn) {
            tempPartitionColumnList.add(setColumnMap(item, map.get(item), partitionColumn.indexOf(item)));
        }
        result.put(MetaDataCons.KEY_PARTITION_COLUMN, tempPartitionColumnList);

        String storedType = null;
        if (map.get(MetaDataCons.KEY_INPUT_FORMAT).keySet().toString().contains(MetaDataCons.TYPE_TEXT)) {
            storedType = "text";
        }
        if (map.get(MetaDataCons.KEY_INPUT_FORMAT).keySet().toString().contains(MetaDataCons.TYPE_PARQUET)) {
            storedType = "Parquet";
        }

        result.put(MetaDataCons.KEY_TABLE, currentQueryTable);

        temp.put(MetaDataCons.KEY_COMMENT, map.get("Table Parameters").get("comment"));
        temp.put(MetaDataCons.KEY_STORED_TYPE, storedType);
        result.put(MetaDataCons.KEY_TABLE_PROPERTIES, temp);

        // 全量读取为createType
        result.put(MetaDataCons.KEY_OPERA_TYPE, "createTable");
        return result;
    }

    /**
     * 通过查询得到的结果构建字段名相应的信息
     */
    public Map<String, Object> setColumnMap(String columnName, Map<String, String> map, int index) {
        Map<String, Object> result = new HashMap<>();
        result.put(MetaDataCons.KEY_COLUMN_NAME, columnName);
        if (map.keySet().toArray()[0] == null) {
            result.put(MetaDataCons.KEY_COLUMN_TYPE, map.keySet().toArray()[1]);
        } else {
            result.put(MetaDataCons.KEY_COLUMN_TYPE, map.keySet().toArray()[0]);
        }
        result.put(MetaDataCons.KEY_COLUMN_INDEX, index);
        if (map.values().toArray()[0] == null) {
            result.put(MetaDataCons.KEY_COLUMN_COMMENT, map.values().toArray()[1]);
        } else {
            result.put(MetaDataCons.KEY_COLUMN_COMMENT, map.values().toArray()[0]);
        }
//        result.put(MetaDataCons.KEY_COLUMN_COMMENT, map.values().toArray()[0]);
        return result;
    }
}
