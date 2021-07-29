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
package com.dtstack.flinkx.mysqld.format;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.rdb.DataSource;
import com.dtstack.flinkx.rdb.datareader.QuerySqlBuilder;
import com.dtstack.flinkx.rdb.inputformat.DistributedJdbcInputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import static com.dtstack.flinkx.rdb.util.DbUtil.clobToString;

/**
 * Date: 2019/09/20
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class MysqldInputFormat extends DistributedJdbcInputFormat {

    @Override
    protected void openNextSource() throws SQLException {
        DataSource currentSource = sourceList.get(sourceIndex);
        currentConn = DbUtil.getConnection(currentSource.getJdbcUrl(), currentSource.getUserName(), currentSource.getPassword());
        currentConn.setAutoCommit(false);
        String queryTemplate = new QuerySqlBuilder(databaseInterface, currentSource.getTable(),metaColumns,splitKey,
                where, currentSource.isSplitByKey(), false, false).buildSql();
        currentStatement = currentConn.createStatement(resultSetType, resultSetConcurrency);

        if (currentSource.isSplitByKey()){
            String n = currentSource.getParameterValues()[0].toString();
            String m = currentSource.getParameterValues()[1].toString();
            queryTemplate = queryTemplate.replace("${N}",n).replace("${M}",m);

            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Executing '%s' with parameters %s", queryTemplate,
                        Arrays.deepToString(currentSource.getParameterValues())));
            }
        }

        currentStatement.setFetchSize(Integer.MIN_VALUE);
        currentStatement.setQueryTimeout(queryTimeOut);
        currentResultSet = currentStatement.executeQuery(queryTemplate);
        columnCount = currentResultSet.getMetaData().getColumnCount();

        if(descColumnTypeList == null) {
            descColumnTypeList = DbUtil.analyzeColumnType(currentResultSet, metaColumns);
        }

        LOG.info("open source: {} ,table: {}", currentSource.getJdbcUrl(), currentSource.getTable());
    }

    @Override
    protected boolean readNextRecord() throws IOException {
        try{
            if(currentConn == null){
                openNextSource();
            }

            hasNext = currentResultSet.next();
            if (hasNext){
                currentRecord = new Row(columnCount);

                for (int pos = 0; pos < currentRecord.getArity(); pos++) {
                    Object obj = currentResultSet.getObject(pos + 1);
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
                    currentRecord.setField(pos, obj);
                }

                if(!ConstantValue.STAR_SYMBOL.equals(metaColumns.get(0).getName())){
                    for (int i = 0; i < columnCount; i++) {
                        Object val = currentRecord.getField(i);
                        if(val == null && metaColumns.get(i).getValue() != null){
                            val = metaColumns.get(i).getValue();
                        }

                        if (val instanceof String){
                            val = StringUtil.string2col(String.valueOf(val),metaColumns.get(i).getType(),metaColumns.get(i).getTimeFormat());
                            currentRecord.setField(i,val);
                        }
                    }
                }
            } else {
                if(sourceIndex + 1 < sourceList.size()){
                    closeCurrentSource();
                    sourceIndex++;
                    return readNextRecord();
                }
            }

            return !hasNext;
        }catch (SQLException se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        } catch (Exception npe) {
            throw new IOException("Couldn't access resultSet", npe);
        }
    }
}
