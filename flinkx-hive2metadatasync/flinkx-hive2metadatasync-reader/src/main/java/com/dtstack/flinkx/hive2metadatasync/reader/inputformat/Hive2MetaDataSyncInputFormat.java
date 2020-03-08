package com.dtstack.flinkx.hive2metadatasync.reader.inputformat;

import com.dtstack.flinkx.metadata.MetaDataCons;
import com.dtstack.flinkx.metadata.reader.inputformat.MetaDataInputFormat;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/8
 * @description :
 */
public class Hive2MetaDataSyncInputFormat extends MetaDataInputFormat {

    protected List<String> tableColumn;
    protected List<String> partitionColumn;


    @Override
    public Map<String, Object> getMetaData(ResultSet resultSet) {
        Map<String, Object> map = new HashMap<>();

        try {
            Map<String, Map<String, String>> temp = transformDataToMap(resultSet);
            getColumn();
            map = selectUsedData(temp);
        } catch (Exception e) {
            setErrorMessage(e, "get Metadata error");
        }
        return map;
    }

    @Override
    public String buildQuerySql(String currentQueryTable) {
        return "DESC FORMATTED " + currentQueryTable;
    }

    public void getColumn() {
        try {
            boolean isPartitionColumn = false;
            tableColumn = new ArrayList<>();
            partitionColumn = new ArrayList<>();
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
        return result;
    }
}
