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

package com.dtstack.flinkx.rdb.inputformat;

import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import com.dtstack.flinkx.inputformat.RichInputFormat;

/**
 * InputFormat for reading data from a database and generate Rows.
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
@Deprecated
public class JdbcInputFormat extends RichInputFormat {

    protected static final long serialVersionUID = 1L;

    protected DatabaseInterface databaseInterface;

    protected String username;

    protected String password;

    protected String drivername;

    protected String dbURL;

    protected String queryTemplate;

    protected int resultSetType;

    protected int resultSetConcurrency;

    protected List<String> descColumnTypeList;

    protected transient Connection dbConn;

    protected transient PreparedStatement statement;

    protected transient ResultSet resultSet;

    protected boolean hasNext;

    protected Object[][] parameterValues;

    protected int columnCount;

    protected String table;

    protected TypeConverterInterface typeConverter;

    protected List<String> column;

    public JdbcInputFormat() {
        resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
       try {
                ClassUtil.forName(drivername, getClass().getClassLoader());
                dbConn = DBUtil.getConnection(dbURL, username, password);

                if(drivername.equalsIgnoreCase("org.postgresql.Driver")){
                    dbConn.setAutoCommit(false);
                }

                 statement = dbConn.prepareStatement(queryTemplate, resultSetType, resultSetConcurrency);

                //提前执行, 否则会由于此连接select数据没有完全返回造成异常|by lgm
                if(descColumnTypeList == null) {
                    descColumnTypeList = DBUtil.analyzeTable(dbConn,databaseInterface,table,column);
                }

                if (inputSplit != null && parameterValues != null) {
                for (int i = 0; i < parameterValues[inputSplit.getSplitNumber()].length; i++) {
                    Object param = parameterValues[inputSplit.getSplitNumber()][i];
                    DBUtil.setParameterValue(param,statement,i);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Executing '%s' with parameters %s", queryTemplate, Arrays.deepToString(parameterValues[inputSplit.getSplitNumber()])));
                }
             }


                //statement.setFetchSize(databaseInterface.getFetchSize());
                //重新设定FetchSize |by lgm
                if(drivername.equalsIgnoreCase("mysqlreader")) {
                    statement.setFetchSize(Integer.MIN_VALUE);
                }
                else {
                    statement.setFetchSize(databaseInterface.getFetchSize());
                }
                statement.setQueryTimeout(databaseInterface.getQueryTimeout());



                resultSet = statement.executeQuery();
                hasNext = resultSet.next();
                columnCount = resultSet.getMetaData().getColumnCount();


            } catch (SQLException se) {
                throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
            }

            LOG.info("JdbcInputFormat[" + jobName + "]open: end");
    }


    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        if (parameterValues == null) {
            return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
        }
        GenericInputSplit[] ret = new GenericInputSplit[parameterValues.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = new GenericInputSplit(i, ret.length);
        }
        return ret;
    }


    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }


    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        row = new Row(columnCount);
        try {
            if (!hasNext) {
                return null;
            }

            DBUtil.getRow(dbURL,row,descColumnTypeList,resultSet,typeConverter);

            //update hasNext after we've read the record
            hasNext = resultSet.next();
            return row;
        } catch (SQLException se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        } catch (NullPointerException npe) {
            throw new IOException("Couldn't access resultSet", npe);
        }
    }

    @Override
    public void closeInternal() throws IOException {
        DBUtil.closeDBResources(resultSet,statement,dbConn);
        parameterValues = null;
    }

}
