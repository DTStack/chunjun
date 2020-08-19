/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.sqlserver.format;

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.sqlserver.SqlServerConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.rdb.util.DbUtil.clobToString;

/**
 * Date: 2019/09/19
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class SqlserverInputFormat extends JdbcInputFormat {

    //是否在sql语句后面添加 with(nolock) ,默认是false
    private Boolean withNoLock;

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        if (!hasNext) {
            return null;
        }
        row = new Row(columnCount);

        try {
            for (int pos = 0; pos < row.getArity(); pos++) {
                Object obj = resultSet.getObject(pos + 1);
                if (obj != null) {
                    if (CollectionUtils.isNotEmpty(descColumnTypeList)) {
                        if ("bit".equalsIgnoreCase(descColumnTypeList.get(pos))) {
                            if (obj instanceof Boolean) {
                                obj = ((Boolean) obj ? 1 : 0);
                            }
                        }
                    }
                    obj = clobToString(obj);
                }

                row.setField(pos, obj);
            }
            return super.nextRecordInternal(row);
        } catch (Exception e) {
            throw new IOException("Couldn't read data - " + e.getMessage(), e);
        }
    }

    /**
     * 构建边界位置sql
     *
     * @param incrementColType 增量字段类型
     * @param incrementCol     增量字段名称
     * @param location         边界位置(起始/结束)
     * @param operator         判断符( >, >=,  <)
     * @return
     */
    @Override
    protected String getLocationSql(String incrementColType, String incrementCol, String location, String operator) {
        String endTimeStr;
        String endLocationSql;
        boolean isTimeType = ColumnType.isTimeType(incrementColType)
                || ColumnType.NVARCHAR.name().equals(incrementColType);
        if (isTimeType) {
            endTimeStr = getTimeStr(Long.parseLong(location), incrementColType);
            endLocationSql = incrementCol + operator + endTimeStr;
        } else if (ColumnType.isNumberType(incrementColType)) {
            endLocationSql = incrementCol + operator + location;
        } else {
            endTimeStr = String.format("'%s'", location);
            endLocationSql = incrementCol + operator + endTimeStr;
        }

        return endLocationSql;
    }

    /**
     * 构建时间边界字符串
     *
     * @param location         边界位置(起始/结束)
     * @param incrementColType 增量字段类型
     * @return
     */
    @Override
    protected String getTimeStr(Long location, String incrementColType) {
        String timeStr;
        Timestamp ts = new Timestamp(DbUtil.getMillis(location));
        ts.setNanos(DbUtil.getNanos(location));
        timeStr = DbUtil.getNanosTimeStr(ts.toString());
        timeStr = timeStr.substring(0, 23);
        timeStr = String.format("'%s'", timeStr);

        return timeStr;
    }

    @Override
    protected List<String> analyzeTable(String dbUrl, String username, String password, DatabaseInterface databaseInterface,
                                        String table, List<MetaColumn> metaColumns) {
        List<String> ret = new ArrayList<>(metaColumns.size());
        Connection dbConn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            dbConn = DbUtil.getConnection(dbUrl, username, password);
            if (null == dbConn) {
                throw new RuntimeException("Get hive connection error");
            }

            stmt = dbConn.createStatement();
            String queryFieldsSql = databaseInterface.getSqlQueryFields(databaseInterface.quoteTable(table));

            //是否需要添加 with(nolock)，添加规则是 from table with(nolock)
            if (getWithNoLock()) {
                //databaseInterface.getSqlQueryFields 返回的结果就是from table  后面没有where等语句所以直接添加的
                queryFieldsSql += SqlServerConstants.WITH_NO_LOCK;
            }
            rs = stmt.executeQuery(queryFieldsSql);
            ResultSetMetaData rd = rs.getMetaData();

            Map<String, String> nameTypeMap = new HashMap<>((rd.getColumnCount() << 2) / 3);
            for (int i = 0; i < rd.getColumnCount(); ++i) {
                nameTypeMap.put(rd.getColumnName(i + 1), rd.getColumnTypeName(i + 1));
            }

            for (MetaColumn metaColumn : metaColumns) {
                if (metaColumn.getValue() != null) {
                    ret.add("string");
                } else {
                    ret.add(nameTypeMap.get(metaColumn.getName()));
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DbUtil.closeDbResources(rs, stmt, dbConn, false);
        }

        return ret;
    }

    public Boolean getWithNoLock() {
        return withNoLock;
    }

    public void setWithNoLock(Boolean withNoLock) {
        this.withNoLock = withNoLock;
    }

}
