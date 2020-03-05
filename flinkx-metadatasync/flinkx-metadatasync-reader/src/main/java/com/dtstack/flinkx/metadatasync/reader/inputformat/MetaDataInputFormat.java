/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.metadatasync.reader.inputformat;

import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/5
 * @description :
 */
public class MetaDataInputFormat extends RichInputFormat {
    protected List<MetaColumn> metaColumns;
    protected String dbUrl;
    protected String username;
    protected String password;
    protected String table;

    protected ResultSet resultSet;
    protected int columnCount;

    protected boolean hasNext = true;

    protected Connection connection;
    protected Statement statement;

    protected List<Map<String, String>> filterData;
    protected int count = 0;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        try {
            excuteSql(buildDescSql(table, true));
            columnCount = resultSet.getMetaData().getColumnCount();
            filterData = trainFormData(resultSet);
            count = filterData.size();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        InputSplit[] inputSplits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return inputSplits;
    }

//    @Override
//    protected Row nextRecordInternal(Row row) throws IOException {
//        try {
//            if (!resultSet.next()) {
//                hasNext = false;
//                return null;
//            }
//            row = new Row(columnCount);
//            for (int pos = 0; pos < columnCount; pos++) {
//                Object obj = resultSet.getObject(pos + 1);
//                if (obj != null) {
//                    obj = DBUtil.clobToString(obj);
//                }
//                row.setField(pos, obj);
//            }
//            hasNext = resultSet.next();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return row;
//    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        if (count == 0) {
            hasNext = false;
            return null;
        }
        row = new Row(2);
        row.setField(0, filterData.get(count - 1).keySet().toString()
                .replace("[", "").replace("]", "").trim());
        row.setField(1, filterData.get(count - 1).values().toString()
                .replace("[", "").replace("]","").trim());
        count--;
        return row;
    }

    @Override
    protected void closeInternal() throws IOException {
        DBUtil.closeDBResources(resultSet, statement, connection, true);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (hasNext) {
            return false;
        }
        return true;
    }

    public void excuteSql(String sql) throws ClassNotFoundException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        try {
            connection = DriverManager.getConnection(dbUrl, username, password);
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String buildDescSql(String table, boolean formatted) {
        String sql = "";
        if (formatted) {
            sql = String.format("DESC FORMATTED " + table);
        } else {
            sql = String.format("DESC " + table);
        }
        return sql;
    }

    public List<Map<String, String>> trainFormData(ResultSet resultSet) {
        List<Map<String, String>> result = new ArrayList<>();
        try {
            while (resultSet.next()) {
                Map<String, String> temp = new HashMap<>();
                String tempKey = (String) resultSet.getObject(1);
                String tempVal = (String) resultSet.getObject(2);
                if (tempKey.contains("#") || (tempKey.isEmpty() && tempVal == null)) {
                    continue;
                }
                if (tempKey.isEmpty() && tempVal != null) {
                    tempKey = (String) resultSet.getObject(2);
                    tempVal = (String) resultSet.getObject(3);
                }
                if (tempVal != null) {
                    tempVal = tempVal.trim();
                }
                temp.put(tempKey.replace(":", "").trim(), tempVal);
                result.add(temp);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
