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

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.datareader.IncrementConfig;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.util.URLUtil;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.HttpClientBuilder;
import org.apache.flink.types.Row;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;

import com.dtstack.flinkx.inputformat.RichInputFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;

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

    protected String splitKey;

    protected int fetchSize;

    protected int queryTimeOut;

    protected int numPartitions;

    protected String customSql;

    protected IncrementConfig incrementConfig;

    protected StringAccumulator tableColAccumulator;

    protected StringAccumulator maxValueAccumulator;

    protected MaximumAccumulator endLocationAccumulator;

    protected StringAccumulator startLocationAccumulator;

    private MetaColumn restoreColumn;

    /**
     * The hadoop config for metric
     */
    protected Map<String,String> hadoopConfig;

    public JdbcInputFormat() {
        resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public void configure(Configuration configuration) {
        if (restoreConfig == null || !restoreConfig.isRestore()){
            return;
        }

        if (restoreConfig.getRestoreColumnIndex() == -1){
            throw new IllegalArgumentException("Restore column index must specified");
        }

        restoreColumn = metaColumns.get(restoreConfig.getRestoreColumnIndex());
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        try {
            LOG.info(inputSplit.toString());

            ClassUtil.forName(drivername, getClass().getClassLoader());

            if (incrementConfig.isIncrement() && !incrementConfig.isUseMaxFunc()){
                getMaxValue(inputSplit);
            }

            initMetric(inputSplit);

            if(!canReadData(inputSplit)){
                LOG.warn("Not read data when the start location are equal to end location");

                hasNext = false;
                return;
            }

            dbConn = DBUtil.getConnection(dbURL, username, password);

            // 部分驱动需要关闭事务自动提交，featchSize参数才会起作用
            dbConn.setAutoCommit(false);

            // 读取前先提交事务，确保程序异常退出时，下次再读取PG时的顺序不变
            if(EDatabaseType.PostgreSQL == databaseInterface.getDatabaseType()){
                dbConn.commit();
            }

            Statement statement = dbConn.createStatement(resultSetType, resultSetConcurrency);
            if(EDatabaseType.MySQL == databaseInterface.getDatabaseType()
                    || EDatabaseType.GBase == databaseInterface.getDatabaseType()){
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

            if (StringUtils.isEmpty(customSql)){
                descColumnTypeList = DBUtil.analyzeTable(dbURL, username, password,databaseInterface,table,metaColumns);
            } else {
                descColumnTypeList = new ArrayList<>();
                for (MetaColumn metaColumn : metaColumns) {
                    descColumnTypeList.add(metaColumn.getName());
                }
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
        JdbcInputSplit[] splits = new JdbcInputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new JdbcInputSplit(i, numPartitions, i, incrementConfig.getStartLocation(), null);
        }

        return splits;
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

            if(incrementConfig.isIncrement() && !incrementConfig.isUseMaxFunc()){
                Object incrementVal = resultSet.getObject(incrementConfig.getColumnIndex() + 1);
                endLocationAccumulator.add(getLocation(incrementConfig.getColumnType(), incrementVal));
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

    private void initMetric(InputSplit split){
        if (!incrementConfig.isIncrement()){
            return;
        }

        Map<String, Accumulator<?, ?>> accumulatorMap = getRuntimeContext().getAllAccumulators();
        if(!accumulatorMap.containsKey(Metrics.TABLE_COL)){
            tableColAccumulator = new StringAccumulator();
            tableColAccumulator.add(table + "-" + incrementConfig.getColumnName());
            getRuntimeContext().addAccumulator(Metrics.TABLE_COL,tableColAccumulator);
        }

        startLocationAccumulator = new StringAccumulator();
        if (incrementConfig.getStartLocation() != null){
            startLocationAccumulator.add(incrementConfig.getStartLocation());
        }
        getRuntimeContext().addAccumulator(Metrics.START_LOCATION,startLocationAccumulator);

        endLocationAccumulator = new MaximumAccumulator();
        String endLocation = ((JdbcInputSplit)split).getEndLocation();
        if(endLocation != null && incrementConfig.isUseMaxFunc()){
            endLocationAccumulator.add(endLocation);
        } else {
            endLocationAccumulator.add(incrementConfig.getStartLocation());
        }
        getRuntimeContext().addAccumulator(Metrics.END_LOCATION,endLocationAccumulator);
    }

    private void getMaxValue(InputSplit inputSplit){
        String maxValue = null;
        if (inputSplit.getSplitNumber() == 0){
            maxValue = getMaxValueFromDb();
            maxValueAccumulator = new StringAccumulator();
            maxValueAccumulator.add(maxValue);
            getRuntimeContext().addAccumulator(Metrics.MAX_VALUE, maxValueAccumulator);
        } else {
            if(StringUtils.isEmpty(monitorUrls)){
                return;
            }

            try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {

                Map<String, String> vars = getRuntimeContext().getMetricGroup().getAllVariables();
                String jobId = vars.get("<job_id>");

                String[] monitors;
                if (monitorUrls.startsWith("http")) {
                    monitors = new String[]{String.format("%s/jobs/%s/accumulators", monitorUrls, jobId)};
                } else {
                    monitors = monitorUrls.split(",");
                    for (int i = 0; i < monitors.length; i++) {
                        monitors[i] = String.format("http://%s/jobs/%s/accumulators", monitors[i], jobId);
                    }
                }

                /**
                 * The extra 10 times is to ensure that accumulator is updated
                 */
                int maxAcquireTimes = (queryTimeOut / incrementConfig.getRequestAccumulatorInterval()) + 10;

                int acquireTimes = 0;
                while (StringUtils.isEmpty(maxValue) && acquireTimes < maxAcquireTimes){
                    try {
                        Thread.sleep(incrementConfig.getRequestAccumulatorInterval() * 1000);
                    } catch (InterruptedException ignore) {
                    }

                    maxValue = getMaxvalueFromAccumulator(httpClient, monitors);
                    acquireTimes++;
                }

                if (StringUtils.isEmpty(maxValue)) {
                    throw new RuntimeException("Can't get the max value from accumulator");
                }
            } catch (IOException e){
                throw new RuntimeException("Can't get the max value from accumulator:" + e);
            }
        }

        ((JdbcInputSplit) inputSplit).setEndLocation(maxValue);
    }

    private String getMaxvalueFromAccumulator(CloseableHttpClient httpClient,String[] monitors){
        String maxValue = null;
        Gson gson = new Gson();
        for (String monitor : monitors) {
            LOG.info("Request url:" + monitor);
            try {
                String response = URLUtil.get(httpClient, monitor);
                Map map = gson.fromJson(response, Map.class);

                LOG.info("Accumulator data:" + gson.toJson(map));

                List<Map> userTaskAccumulators = (List<Map>) map.get("user-task-accumulators");
                for (Map accumulator : userTaskAccumulators) {
                    if (Metrics.MAX_VALUE.equals(accumulator.get("name"))) {
                        maxValue = (String) accumulator.get("value");
                        break;
                    }
                }

                if (StringUtils.isNotEmpty(maxValue)) {
                    break;
                }
            } catch (Exception e) {
                LOG.error("Get max value from accumulator error:", e);
            }
        }

        return maxValue;
    }

    private boolean canReadData(InputSplit split){
        if (!incrementConfig.isIncrement()){
            return true;
        }

        JdbcInputSplit jdbcInputSplit = (JdbcInputSplit) split;
        return !StringUtils.equals(jdbcInputSplit.getStartLocation(), jdbcInputSplit.getEndLocation());
    }

    private String buildQuerySql(InputSplit inputSplit){
        String querySql = queryTemplate;

        if (inputSplit == null){
            LOG.warn(String.format("Executing sql is: '%s'", querySql));
            return querySql;
        }

        JdbcInputSplit jdbcInputSplit = (JdbcInputSplit) inputSplit;

        if (StringUtils.isNotEmpty(splitKey)){
            querySql = queryTemplate.replace("${N}", String.valueOf(numPartitions))
                    .replace("${M}", String.valueOf(indexOfSubtask));
        }

        if (restoreConfig.isRestore()){
            if(formatState == null){
                querySql = querySql.replace(DBUtil.RESTORE_FILTER_PLACEHOLDER, StringUtils.EMPTY);

                if (incrementConfig.isIncrement()){
                    querySql = buildIncrementSql(jdbcInputSplit, querySql);
                }
            } else {
                String startLocation = getLocation(restoreColumn.getType(), formatState.getState());
                String restoreFilter = DBUtil.buildIncrementFilter(databaseInterface, restoreColumn.getType(),
                        restoreColumn.getName(), startLocation, jdbcInputSplit.getEndLocation(), customSql, incrementConfig.isUseMaxFunc());

                if(StringUtils.isNotEmpty(restoreFilter)){
                    restoreFilter = " and " + restoreFilter;
                }

                querySql = querySql.replace(DBUtil.RESTORE_FILTER_PLACEHOLDER, restoreFilter);
            }

            querySql = querySql.replace(DBUtil.INCREMENT_FILTER_PLACEHOLDER, StringUtils.EMPTY);
        } else if (incrementConfig.isIncrement()){
            querySql = buildIncrementSql(jdbcInputSplit, querySql);
        }

        LOG.warn(String.format("Executing sql is: '%s'", querySql));

        return querySql;
    }

    private String buildIncrementSql(JdbcInputSplit jdbcInputSplit, String querySql){
        String incrementFilter = DBUtil.buildIncrementFilter(databaseInterface, incrementConfig.getColumnType(),
                incrementConfig.getColumnName(), jdbcInputSplit.getStartLocation(),
                jdbcInputSplit.getEndLocation(), customSql, incrementConfig.isUseMaxFunc());

        if(StringUtils.isNotEmpty(incrementFilter)){
            incrementFilter = " and " + incrementFilter;
        }

        return querySql.replace(DBUtil.INCREMENT_FILTER_PLACEHOLDER, incrementFilter);
    }

    private String getMaxValueFromDb() {
        String maxValue = null;
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            long startTime = System.currentTimeMillis();

            String queryMaxValueSql;
            if (StringUtils.isNotEmpty(customSql)){
                queryMaxValueSql = String.format("select max(%s.%s) as max_value from ( %s ) %s", DBUtil.TEMPORARY_TABLE_NAME,
                        databaseInterface.quoteColumn(incrementConfig.getColumnName()), customSql, DBUtil.TEMPORARY_TABLE_NAME);
            } else {
                queryMaxValueSql = String.format("select max(%s) as max_value from %s",
                        databaseInterface.quoteColumn(incrementConfig.getColumnName()), databaseInterface.quoteTable(table));
            }

            String startSql = DBUtil.buildStartLocationSql(databaseInterface, incrementConfig.getColumnType(),
                    databaseInterface.quoteColumn(incrementConfig.getColumnName()), incrementConfig.getStartLocation(), incrementConfig.isUseMaxFunc());
            if(StringUtils.isNotEmpty(startSql)){
                queryMaxValueSql += " where " + startSql;
            }

            LOG.info(String.format("Query max value sql is '%s'", queryMaxValueSql));

            conn = DBUtil.getConnection(dbURL, username, password);
            st = conn.createStatement();
            rs = st.executeQuery(queryMaxValueSql);
            if (rs.next()){
                maxValue = getLocation(incrementConfig.getColumnType(), rs.getObject("max_value"));
            }

            LOG.info(String.format("Takes [%s] milliseconds to get the maximum value [%s]", System.currentTimeMillis() - startTime, maxValue));

            return maxValue;
        } catch (Throwable e){
            throw new RuntimeException("Get max value from " + table + " error",e);
        } finally {
            DBUtil.closeDBResources(rs, st, conn, false);
        }
    }

    private String getLocation(String columnType, Object columnVal){
        String location;
        if (columnVal == null){
            return null;
        }

        if (ColumnType.isTimeType(columnType)){
            if(columnVal instanceof Long){
                location = columnVal.toString();
            } else if(columnVal instanceof Timestamp){
                long time = ((Timestamp)columnVal).getTime() / 1000;

                String nanosStr = String.valueOf(((Timestamp)columnVal).getNanos());
                if(nanosStr.length() == 9){
                    location = time + nanosStr;
                } else {
                    String fillZeroStr = StringUtils.repeat("0",9 - nanosStr.length());
                    location = time + fillZeroStr + nanosStr;
                }
            } else {
                Date date = DateUtil.stringToDate(columnVal.toString(),null);
                String fillZeroStr = StringUtils.repeat("0",6);
                long time = date.getTime();
                location = time + fillZeroStr;
            }
        } else if(ColumnType.isNumberType(columnType)){
            location = String.valueOf(columnVal);
        } else {
            location = String.valueOf(columnVal);
        }

        return location;
    }

    private void uploadMetricData() throws IOException {
        FSDataOutputStream out = null;
        try {
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

            if(hadoopConfig != null) {
                for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
                    conf.set(entry.getKey(), entry.getValue());
                }
            }

            Map<String, String> vars = getRuntimeContext().getMetricGroup().getAllVariables();
            String jobId = vars.get("<job_id>");
            String taskId = vars.get("<task_id>");
            String subtaskIndex = vars.get("<subtask_index>");
            LOG.info("jobId:{} taskId:{} subtaskIndex:{}", jobId, taskId, subtaskIndex);

            Path remotePath = new Path(conf.get("fs.defaultFS"), "/tmp/logs/admin/logs/" + jobId + "/" + taskId + "_" + subtaskIndex);
            out = FileSystem.create(remotePath.getFileSystem(conf), remotePath, new FsPermission(FsPermission.createImmutable((short) 0777)));

            Map<String,Object> metrics = new HashMap<>(3);
            metrics.put(Metrics.TABLE_COL, table + "-" + incrementConfig.getColumnName());
            if (startLocationAccumulator != null){
                metrics.put(Metrics.START_LOCATION, startLocationAccumulator.getLocalValue());
            }
            if (endLocationAccumulator != null){
                metrics.put(Metrics.END_LOCATION, endLocationAccumulator.getLocalValue());
            }
            out.writeUTF(new ObjectMapper().writeValueAsString(metrics));
        } finally {
            IOUtils.closeStream(out);
        }
    }

    @Override
    public void closeInternal() throws IOException {
        if(incrementConfig.isIncrement() && hadoopConfig != null) {
            uploadMetricData();
        }
        DBUtil.closeDBResources(resultSet,statement,dbConn, true);
    }

}