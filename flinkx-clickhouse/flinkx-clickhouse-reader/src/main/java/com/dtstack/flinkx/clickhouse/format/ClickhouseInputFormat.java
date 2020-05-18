/*
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
package com.dtstack.flinkx.clickhouse.format;

import com.dtstack.flinkx.clickhouse.core.ClickhouseUtil;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import static com.dtstack.flinkx.rdb.util.DbUtil.clobToString;

/**
 * Date: 2019/11/05
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class ClickhouseInputFormat extends JdbcInputFormat {

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        try {
            LOG.info("inputSplit = {}", inputSplit);
            ClassUtil.forName(driverName, getClass().getClassLoader());
            dbConn = ClickhouseUtil.getConnection(dbUrl, username, password);
            initMetric(inputSplit);
            String startLocation = incrementConfig.getStartLocation();
            if (incrementConfig.isPolling()) {
                endLocationAccumulator.add(Long.parseLong(startLocation));
                isTimestamp = "timestamp".equalsIgnoreCase(incrementConfig.getColumnType());
            } else if ((incrementConfig.isIncrement() && incrementConfig.isUseMaxFunc())) {
                getMaxValue(inputSplit);
            }

            if(!canReadData(inputSplit)){
                LOG.warn("Not read data when the start location are equal to end location");
                hasNext = false;
                return;
            }

            Statement statement = dbConn.createStatement(resultSetType, resultSetConcurrency);
            statement.setFetchSize(fetchSize);
            statement.setQueryTimeout(queryTimeOut);
            String querySql = buildQuerySql(inputSplit);
            resultSet = statement.executeQuery(querySql);
            columnCount = resultSet.getMetaData().getColumnCount();

            boolean splitWithRowCol = numPartitions > 1 && StringUtils.isNotEmpty(splitKey) && splitKey.contains("(");
            if(splitWithRowCol){
                columnCount = columnCount-1;
            }

            hasNext = resultSet.next();

            if (StringUtils.isEmpty(customSql)){
                descColumnTypeList = DbUtil.analyzeTable(dbUrl, username, password, databaseInterface, table, metaColumns);
            } else {
                descColumnTypeList = new ArrayList<>();
                for (MetaColumn metaColumn : metaColumns) {
                    descColumnTypeList.add(metaColumn.getName());
                }
            }
        } catch (Exception e) {
            LOG.error("open failed,e = {}", ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(e);
        }

        LOG.info("JdbcInputFormat[{}]open: end", jobName);
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        if (!hasNext) {
            return null;
        }
        row = new Row(columnCount);
        try {
            for (int pos = 0; pos < row.getArity(); pos++) {
                Object obj = resultSet.getObject(pos + 1);
                if(obj != null) {
                    obj = clobToString(obj);
                }
                row.setField(pos, obj);
            }
            return super.nextRecordInternal(row);
        } catch (SQLException se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        } catch (Exception npe) {
            throw new IOException("Couldn't access resultSet", npe);
        }
    }
}
