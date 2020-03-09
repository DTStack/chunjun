package com.dtstack.flinkx.metadata.hive2.inputformat;

import com.dtstack.flinkx.metadata.MetaDataCons;
import com.dtstack.flinkx.metadata.hive2.common.Hive2MetaDataCons;
import com.dtstack.flinkx.metadata.reader.inputformat.MetaDataInputFormat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author : tiezhu
 * @date : 2020/3/9
 */
public class Hive2MetadataInputFormat extends MetaDataInputFormat {
    protected List<String> tableColumn;
    protected List<String> partitionColumn;

    protected Map<String, Object> columnMap;

    @Override
    protected void beforeUnit(String currentQueryTable) {
        getColumn();
        columnMap = transformDataToMap(executeSql(buildDescSql(currentQueryTable, false)));
    }

    @Override
    public Map<String, Object> getTablePropertites(String currentQueryTable) {
        ResultSet resultSet = executeSql(buildDescSql(currentQueryTable, true));
        Map<String, Object> result;
        Map<String, Object> temp = new HashMap<>();
        result = transformDataToMap(resultSet);
        cleanRawData(result);

        // 对result遍历，重新组合
        for (Map.Entry<String, Object> stringObjectEntry : result.entrySet()) {
            temp.put(toLowerCaseFirstOne((String) ((Map.Entry) stringObjectEntry).getKey()), ((Map) stringObjectEntry.getValue()).keySet()
                    .toString().replace("[", "")
                    .replace("]", "").trim());
        }
        if (temp.get("inputFormat").toString().contains("TextInputFormat")) {
            temp.put("storedType", "text");
        }
        return temp;
    }

    @Override
    public Map<String, Object> getColumnPropertites(String currentQueryTable) {
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> tempColumnList = new ArrayList<>();

        for (String item : tableColumn) {
            tempColumnList.add(setColumnMap(item, columnMap.get(item), tableColumn.indexOf(item)));
        }
        result.put(MetaDataCons.KEY_COLUMN, tempColumnList);
        return result;
    }

    @Override
    public Map<String, Object> getPartitionPropertites(String currentQueryTable) {
        Map<String, Object> result = new HashMap<>();
        try {
            List<Map<String, Object>> tempPartitionColumnList = new ArrayList<Map<String, Object>>();

            for (String item : partitionColumn) {
                tempPartitionColumnList.add(setColumnMap(item, columnMap.get(item), partitionColumn.indexOf(item)));
            }
            result.put(Hive2MetaDataCons.KEY_PARTITION_COLUMN, tempPartitionColumnList);
            result.put("partitions", getPartitions(currentQueryTable));
        } catch (Exception e) {
            setErrorMessage(e, "get partitions error");
        }
        return result;
    }

    public void cleanRawData(Map<String, Object> map) {
        //过滤无关信息和空值
        cleanData(map);
        // 过滤表字段的相关信息
        for (String item : tableColumn) {
            map.remove(item);
        }
    }

    /**
     * 去除无关数据
     */
    public void cleanData(Map<String, Object> map) {
        Iterator<Map.Entry<String, Object>> entries = map.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, Object> entry = entries.next();
            if (entry.getKey().contains("#")) {
                entries.remove();
                continue;
            }
            ((Map<String, String>) entry.getValue()).remove(null);
        }
    }

    public void cleanData(List<String> list) {
        list.removeIf(item -> item.contains("#") || item.isEmpty());
    }

    /**
     * 构建查询语句
     */
    public String buildDescSql(String currentQueryTable, boolean formatted) {
        String sql;
        if (formatted) {
            sql = "DESC FORMATTED " + currentQueryTable;
        } else {
            sql = "DESC " + currentQueryTable;
        }
        return sql;
    }

    /**
     * 获取表中字段名称，包括分区字段和非分区字段
     */
    public void getColumn() {
        try {
            boolean isPartitionColumn = false;
            tableColumn = new ArrayList<>();
            partitionColumn = new ArrayList<>();
            ResultSet temp = executeSql(buildDescSql(currentQueryTable, false));
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
     * 从查询结果中构建Map
     */
    public Map<String, Object> transformDataToMap(ResultSet resultSet) {
        Map<String, Object> result = new HashMap<>();
        String tempK = "";
        Map<String, String> map = new HashMap<>();
        try {
            while (resultSet.next()) {
                String tempK1 = resultSet.getString(1)
                        .replace(":", "").trim();
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
        } catch (Exception e) {
            setErrorMessage(e, "transform data error");
        }
        return result;
    }

    /**
     * 通过查询得到的结果构建字段名相应的信息
     */
    public Map<String, Object> setColumnMap(String columnName, Object map, int index) {
        HashMap<String, String> temp = (HashMap<String, String>) map;
        Map<String, Object> result = new HashMap<>();
        result.put(MetaDataCons.KEY_COLUMN_NAME, columnName);
        if (temp.keySet().toArray()[0] == null) {
            result.put(MetaDataCons.KEY_COLUMN_TYPE, temp.keySet().toArray()[1]);
        } else {
            result.put(MetaDataCons.KEY_COLUMN_TYPE, temp.keySet().toArray()[0]);
        }
        result.put(MetaDataCons.KEY_COLUMN_INDEX, index);
        if (temp.values().toArray()[0] == null) {
            result.put(MetaDataCons.KEY_COLUMN_COMMENT, temp.values().toArray()[1]);
        } else {
            result.put(MetaDataCons.KEY_COLUMN_COMMENT, temp.values().toArray()[0]);
        }
        return result;
    }

    /**
     * 获取分区partitions
     */
    public List<String> getPartitions(String currentTable) throws SQLException {
        List<String> partitions = new ArrayList<>();
        ResultSet resultSet = executeSql("show partitions " + currentTable);
        while (resultSet.next()) {
            partitions.add(resultSet.getString(1));
        }
        return partitions;
    }

    /**
     * 将字符串首字母转小写
     */
    public String toLowerCaseFirstOne(String s) {
        if (Character.isLowerCase(s.charAt(0))) {
            return s;
        } else {
            return Character.toLowerCase(s.charAt(0)) + s.substring(1);
        }
    }
}
