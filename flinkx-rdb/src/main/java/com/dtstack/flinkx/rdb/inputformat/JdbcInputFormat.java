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

import com.dtstack.flinkx.common.ColumnType;
import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;

import com.dtstack.flinkx.inputformat.RichInputFormat;

/**
 * InputFormat for reading data from a database and generate Rows.
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
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

    protected transient Statement statement;

    protected transient ResultSet resultSet;

    protected boolean hasNext;

    protected int columnCount;

    protected String table;

    protected TypeConverterInterface typeConverter;

    protected List<MetaColumn> metaColumns;

    protected String increCol;

    protected String increColType;

    protected String startLocation;

    protected String splitKey;

    private int increColIndex;

    protected int fetchSize;

    protected int queryTimeOut;

    protected boolean realTimeIncreSync;

    protected int numPartitions;

    protected StringAccumulator tableColAccumulator;

    protected MaximumAccumulator endLocationAccumulator;

    protected StringAccumulator startLocationAccumulator;

    public JdbcInputFormat() {
        resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public void configure(Configuration configuration) {

    }

    private void initMetric(InputSplit split){

        if (StringUtils.isEmpty(increCol)){
            return;
        }

        Map<String, Accumulator<?, ?>> accumulatorMap = getRuntimeContext().getAllAccumulators();

        if(!accumulatorMap.containsKey(Metrics.TABLE_COL)){
            tableColAccumulator = new StringAccumulator();
            tableColAccumulator.add(table + "-" + increCol);
            getRuntimeContext().addAccumulator(Metrics.TABLE_COL,tableColAccumulator);
        }

        String endLocation = ((JdbcInputSplit)split).getEndLocation();
        if(!accumulatorMap.containsKey(Metrics.END_LOCATION) && endLocation != null){
            endLocationAccumulator = new MaximumAccumulator();

            if(realTimeIncreSync){
                endLocationAccumulator.add(endLocation);
            }

            getRuntimeContext().addAccumulator(Metrics.END_LOCATION,endLocationAccumulator);
        }

        if (!accumulatorMap.containsKey(Metrics.START_LOCATION) && startLocation != null){
            if(!realTimeIncreSync){
                endLocationAccumulator.add(startLocation);
            }

            startLocationAccumulator = new StringAccumulator();
            startLocationAccumulator.add(startLocation);
            getRuntimeContext().addAccumulator(Metrics.START_LOCATION,startLocationAccumulator);
        }

        for (int i = 0; i < metaColumns.size(); i++) {
            if (metaColumns.get(i).getName().equals(increCol)){
                increColIndex = i;
                break;
            }
        }
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        try {
            LOG.info(inputSplit.toString());

            initMetric(inputSplit);

            if(!canReadData(inputSplit)){
                LOG.warn("Not read data when the start location are equal to end location");

                hasNext = false;
                return;
            }

            ClassUtil.forName(drivername, getClass().getClassLoader());
            dbConn = DBUtil.getConnection(dbURL, username, password);
            dbConn.setAutoCommit(false);
            Statement statement = dbConn.createStatement(resultSetType, resultSetConcurrency);

            if(EDatabaseType.MySQL == databaseInterface.getDatabaseType()){
                statement.setFetchSize(Integer.MIN_VALUE);
            } else {
                statement.setFetchSize(fetchSize);
            }

            if(EDatabaseType.Carbondata != databaseInterface.getDatabaseType()) {
                statement.setQueryTimeout(queryTimeOut);
            }

            String querySql = buildQuerySql(inputSplit);
            resultSet = statement.executeQuery(querySql);
            columnCount = resultSet.getMetaData().getColumnCount();
            hasNext = resultSet.next();

            if(descColumnTypeList == null) {
                descColumnTypeList = DBUtil.analyzeTable(dbURL, username, password,databaseInterface,table,metaColumns);
            }

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
        String endLocation = null;
        if (realTimeIncreSync){
            endLocation = getEndLocation();

            if(StringUtils.equals(startLocation, endLocation)){
                JdbcInputSplit[] splits = new JdbcInputSplit[1];
                splits[0] = new JdbcInputSplit(0, numPartitions, 0, startLocation, endLocation);

                return splits;
            }
        }

        JdbcInputSplit[] splits = new JdbcInputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new JdbcInputSplit(i, numPartitions, i, startLocation, endLocation);
        }

        return splits;
    }

    private boolean canReadData(InputSplit split){
        if (StringUtils.isEmpty(increCol)){
            return true;
        }

        if (!realTimeIncreSync){
            return true;
        }

        JdbcInputSplit jdbcInputSplit = (JdbcInputSplit) split;
        return !StringUtils.equals(jdbcInputSplit.getStartLocation(), jdbcInputSplit.getEndLocation());
    }

    private String buildQuerySql(InputSplit inputSplit){
        String querySql = queryTemplate;

        if (inputSplit != null) {
            JdbcInputSplit jdbcInputSplit = (JdbcInputSplit) inputSplit;

            if (StringUtils.isNotEmpty(splitKey)){
                querySql = queryTemplate.replace("${N}", String.valueOf(numPartitions))
                        .replace("${M}", String.valueOf(jdbcInputSplit.getMod()));
            }

            if (realTimeIncreSync){
                String incrementFilter = DBUtil.buildIncrementFilter(databaseInterface, increColType, increCol,
                        jdbcInputSplit.getStartLocation(), jdbcInputSplit.getEndLocation());

                if(StringUtils.isNotEmpty(incrementFilter)){
                    incrementFilter = " and " + incrementFilter;
                }

                querySql = querySql.replace(DBUtil.INCREMENT_FILTER_PLACEHOLDER, incrementFilter);
            }
        }

        LOG.warn(String.format("Executing sql is: '%s'", querySql));

        return querySql;
    }

    private String getEndLocation() {
        String maxValue = null;
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            String queryMaxValueSql = String.format("select max(%s) as max_value from %s", increCol, table);
            String startSql = DBUtil.buildStartLocationSql(databaseInterface, increColType, increCol, startLocation);
            if(StringUtils.isNotEmpty(startSql)){
                queryMaxValueSql += " where " + startSql;
            }

            LOG.info(String.format("Query max value sql is '%s'", queryMaxValueSql));

            ClassUtil.forName(drivername, getClass().getClassLoader());
            conn = DBUtil.getConnection(dbURL, username, password);
            st = conn.createStatement();
            rs = st.executeQuery(queryMaxValueSql);
            if (rs.next()){
                if (ColumnType.isTimeType(increColType)){
                    Timestamp increVal = rs.getTimestamp("max_value");
                    if(increVal != null){
                        maxValue = String.valueOf(getLocation(increVal));
                    }
                } else if(ColumnType.isNumberType(increColType)){
                    maxValue = String.valueOf(rs.getLong("max_value"));
                } else {
                    maxValue = rs.getString("max_value");
                }
            }

            LOG.info(String.format("The max value of column %s is %s", increCol, maxValue));

            return maxValue;
        } catch (Throwable e){
            throw new RuntimeException("Get max value from " + table + " error",e);
        } finally {
            DBUtil.closeDBResources(rs,st,conn);
        }
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

            DBUtil.getRow(databaseInterface.getDatabaseType(),row,descColumnTypeList,resultSet,typeConverter);
            if(!"*".equals(metaColumns.get(0).getName())){
                for (int i = 0; i < columnCount; i++) {
                    Object val = row.getField(i);
                    if(val == null && metaColumns.get(i).getValue() != null){
                        val = metaColumns.get(i).getValue();
                    }

                    if (val instanceof String){
                        val = StringUtil.string2col(String.valueOf(val),metaColumns.get(i).getType(),metaColumns.get(i).getTimeFormat());
                        row.setField(i,val);
                    }
                }
            }

            if(increCol != null && !realTimeIncreSync){
                if (ColumnType.isTimeType(increColType)){
                    Timestamp increVal = resultSet.getTimestamp(increColIndex + 1);
                    if(increVal != null){
                        endLocationAccumulator.add(String.valueOf(getLocation(increVal)));
                    }
                } else if(ColumnType.isNumberType(increColType)){
                    endLocationAccumulator.add(String.valueOf(resultSet.getLong(increColIndex + 1)));
                } else {
                    String increVal = resultSet.getString(increColIndex + 1);
                    if(increVal != null){
                        endLocationAccumulator.add(increVal);
                    }
                }
            }

            //update hasNext after we've read the record
            hasNext = resultSet.next();
            return row;
        } catch (SQLException se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        } catch (Exception npe) {
            throw new IOException("Couldn't access resultSet", npe);
        }
    }

    private long getLocation(Object increVal){
        if(increVal instanceof Timestamp){
            long time = ((Timestamp)increVal).getTime() / 1000;

            String nanosStr = String.valueOf(((Timestamp)increVal).getNanos());
            if(nanosStr.length() == 9){
                return Long.parseLong(time + nanosStr);
            } else {
                String fillZeroStr = StringUtils.repeat("0",9 - nanosStr.length());
                return Long.parseLong(time + fillZeroStr + nanosStr);
            }
        } else {
            Date date = DateUtil.stringToDate(increVal.toString(),null);
            String fillZeroStr = StringUtils.repeat("0",6);
            long time = date.getTime();
            return Long.parseLong(time + fillZeroStr);
        }
    }

    @Override
    public void closeInternal() throws IOException {
        DBUtil.closeDBResources(resultSet,statement,dbConn);
    }

}