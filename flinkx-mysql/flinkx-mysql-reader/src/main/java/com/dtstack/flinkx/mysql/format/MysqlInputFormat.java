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
package com.dtstack.flinkx.mysql.format;

import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import static com.dtstack.flinkx.rdb.util.DbUtil.clobToString;

/**
 * Date: 2019/09/19
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class MysqlInputFormat extends JdbcInputFormat {

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        try {
            LOG.info("inputSplit = {}", inputSplit);

            ClassUtil.forName(driverName, getClass().getClassLoader());
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

            querySql = buildQuerySql(inputSplit);
            //MySQL流式读取
            fetchSize = Integer.MIN_VALUE;
            executeQuery(startLocation);
            columnCount = resultSet.getMetaData().getColumnCount();

            boolean splitWithRowCol = numPartitions > 1 && StringUtils.isNotEmpty(splitKey) && splitKey.contains("(");
            if(splitWithRowCol){
                columnCount = columnCount-1;
            }

            if (StringUtils.isEmpty(customSql)){
                descColumnTypeList = DbUtil.analyzeTable(dbUrl, username, password,databaseInterface,table,metaColumns);
            } else {
                descColumnTypeList = new ArrayList<>();
                for (MetaColumn metaColumn : metaColumns) {
                    descColumnTypeList.add(metaColumn.getName());
                }
            }

        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed. " + se.getMessage(), se);
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
                    if(CollectionUtils.isNotEmpty(descColumnTypeList)) {
                        String columnType = descColumnTypeList.get(pos);
                        if("year".equalsIgnoreCase(columnType)) {
                            java.util.Date date = (java.util.Date) obj;
                            obj = DateUtil.dateToYearString(date);
                        } else if("tinyint".equalsIgnoreCase(columnType)
                                    || "bit".equalsIgnoreCase(columnType)) {
                            if(obj instanceof Boolean) {
                                obj = ((Boolean) obj ? 1 : 0);
                            }
                        }
                    }
                    obj = clobToString(obj);
                }

                row.setField(pos, obj);
            }
            return super.nextRecordInternal(row);
        }catch (Exception e) {
            LOG.error("error to get next record, row = {}, descColumnTypeList = {}, e = {}", row, new Gson().toJson(descColumnTypeList), ExceptionUtil.getErrorMessage(e));
            throw new IOException("Couldn't read data - " + e.getMessage(), e);
        }
    }

}
