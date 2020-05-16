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

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.datareader.IncrementConfig;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.*;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.util.UrlUtil;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongMaximum;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * InputFormat for reading data from a database and generate Rows.
 * <p>
 * Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class JdbcInputFormat extends BaseRichInputFormat {

    protected static final long serialVersionUID = 1L;

    protected DatabaseInterface databaseInterface;

    protected String username;

    protected String password;

    protected String driverName;

    protected String dbUrl;

    protected String queryTemplate;

    protected int resultSetType;

    protected int resultSetConcurrency;

    protected List<String> descColumnTypeList;

    protected transient Connection dbConn;

    protected transient Statement statement;

    protected transient PreparedStatement ps;

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

    protected StringAccumulator maxValueAccumulator;

    protected LongMaximum endLocationAccumulator;

    protected LongMaximum startLocationAccumulator;

    protected MetaColumn restoreColumn;

    protected Row lastRow = null;

    protected String querySql;

    /**
     * 轮询增量标识字段类型，目前只有timestamp类型和数值类型
     */
    protected boolean isTimestamp = false;

    /**
     * The hadoop config for metric
     */
    protected Map<String, Object> hadoopConfig;

    public JdbcInputFormat() {
        resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        if (restoreConfig == null || !restoreConfig.isRestore()){
            return;
        }

        if (restoreConfig.getRestoreColumnIndex() == -1) {
            throw new IllegalArgumentException("Restore column index must specified");
        }

        restoreColumn = metaColumns.get(restoreConfig.getRestoreColumnIndex());
    }

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

            if (!canReadData(inputSplit)) {
                LOG.warn("Not read data when the start location are equal to end location");
                hasNext = false;
                return;
            }

            querySql = buildQuerySql(inputSplit);
            JdbcInputSplit jdbcInputSplit = (JdbcInputSplit) inputSplit;
            if (null != jdbcInputSplit.getStartLocation()) {
                startLocation = jdbcInputSplit.getStartLocation();
            }
            executeQuery(startLocation);

            columnCount = resultSet.getMetaData().getColumnCount();

            boolean splitWithRowCol = numPartitions > 1 && StringUtils.isNotEmpty(splitKey) && splitKey.contains("(");
            if (splitWithRowCol) {
                columnCount = columnCount - 1;
            }

            if (StringUtils.isEmpty(customSql)) {
                descColumnTypeList = DbUtil.analyzeTable(dbUrl, username, password, databaseInterface, table, metaColumns);
            } else {
                descColumnTypeList = new ArrayList<>();
                for (MetaColumn metaColumn : metaColumns) {
                    descColumnTypeList.add(metaColumn.getName());
                }
            }

        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }

        LOG.info("JdbcInputFormat[{}]open: end", jobName);
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws IOException {
        JdbcInputSplit[] splits = new JdbcInputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new JdbcInputSplit(i, numPartitions, i, incrementConfig.getStartLocation(), null);
        }

        return splits;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (hasNext) {
            return false;
        } else {
            if (incrementConfig.isPolling()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(incrementConfig.getPollingInterval());
                    if(!dbConn.getAutoCommit()){
                        dbConn.setAutoCommit(true);
                    }
                    DbUtil.closeDbResources(resultSet, null, null, false);
                    queryForPolling(endLocationAccumulator.getLocalValue().toString());
                    return false;
                } catch (InterruptedException e) {
                    LOG.warn("interrupted while waiting for polling, e = {}", ExceptionUtil.getErrorMessage(e));
                } catch (SQLException e) {
                    LOG.error("error to execute sql = {}, startLocation = {}, e = {}", querySql, endLocationAccumulator.getLocalValue(), ExceptionUtil.getErrorMessage(e));
                    DbUtil.closeDbResources(resultSet, ps, null, false);
                    throw new RuntimeException(e);
                }
            }
            return true;
        }
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        try {
            if (!ConstantValue.STAR_SYMBOL.equals(metaColumns.get(0).getName())) {
                for (int i = 0; i < columnCount; i++) {
                    Object val = row.getField(i);
                    if (val == null && metaColumns.get(i).getValue() != null) {
                        val = metaColumns.get(i).getValue();
                    }

                    if (val instanceof String) {
                        val = StringUtil.string2col(String.valueOf(val), metaColumns.get(i).getType(), metaColumns.get(i).getTimeFormat());
                        row.setField(i, val);
                    }
                }
            }

            boolean isUpdateLocation = incrementConfig.isPolling() || (incrementConfig.isIncrement() && !incrementConfig.isUseMaxFunc());
            if (isUpdateLocation) {
                Object incrementVal = resultSet.getObject(incrementConfig.getColumnName());
                String location;
                if(incrementConfig.isPolling()){
                    location = String.valueOf(incrementVal);
                }else{
                    location = getLocation(incrementConfig.getColumnType(), incrementVal);
                }

                if(StringUtils.isNotEmpty(location)) {
                    endLocationAccumulator.add(Long.parseLong(location));
                }

                LOG.trace("update endLocationAccumulator, current Location = {}", location);
            }

            //update hasNext after we've read the record
            hasNext = resultSet.next();

            if (restoreConfig.isRestore()) {
                lastRow = row;
            }

            return row;
        } catch (SQLException se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        } catch (Exception npe) {
            throw new IOException("Couldn't access resultSet", npe);
        }
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();

        if (formatState != null && lastRow != null) {
            formatState.setState(lastRow.getField(restoreConfig.getRestoreColumnIndex()));
        }
        return formatState;
    }

    @Override
    public void closeInternal() throws IOException {
        if (incrementConfig.isIncrement() && hadoopConfig != null) {
            uploadMetricData();
        }
        DbUtil.closeDbResources(resultSet, statement, dbConn, true);
    }

    /**
     * 初始化增量任务指标
     *
     * @param split 数据分片
     */
    protected void initMetric(InputSplit split) {
        if (!incrementConfig.isIncrement()) {
            return;
        }

        startLocationAccumulator = new LongMaximum();
        if (StringUtils.isNotEmpty(incrementConfig.getStartLocation())) {
            startLocationAccumulator.add(Long.parseLong(incrementConfig.getStartLocation()));
        }
        customPrometheusReporter.registerMetric(startLocationAccumulator, Metrics.START_LOCATION);
        getRuntimeContext().addAccumulator(Metrics.START_LOCATION, startLocationAccumulator);
        endLocationAccumulator = new LongMaximum();
        String endLocation = ((JdbcInputSplit) split).getEndLocation();
        if (endLocation != null && incrementConfig.isUseMaxFunc()) {
            endLocationAccumulator.add(Long.parseLong(endLocation));
        } else if (StringUtils.isNotEmpty(incrementConfig.getStartLocation())) {
            endLocationAccumulator.add(Long.parseLong(incrementConfig.getStartLocation()));
        }
        customPrometheusReporter.registerMetric(endLocationAccumulator, Metrics.END_LOCATION);
        getRuntimeContext().addAccumulator(Metrics.END_LOCATION, endLocationAccumulator);
    }

    /**
     * 使用自定义的指标输出器把增量指标打到普罗米修斯
     */
    @Override
    protected boolean useCustomPrometheusReporter() {
        return incrementConfig.isIncrement();
    }

    /**
     * 为了保证增量数据的准确性，指标输出失败时使任务失败
     */
    @Override
    protected boolean makeTaskFailedWhenReportFailed(){
        return true;
    }

    /**
     * 将增量任务的数据最大值设置到累加器中
     *
     * @param inputSplit 数据分片
     */
    protected void getMaxValue(InputSplit inputSplit) {
        String maxValue = null;
        if (inputSplit.getSplitNumber() == 0) {
            maxValue = getMaxValueFromDb();
            maxValueAccumulator = new StringAccumulator();
            maxValueAccumulator.add(maxValue);
            getRuntimeContext().addAccumulator(Metrics.MAX_VALUE, maxValueAccumulator);
        } else {
            maxValue = getMaxValueFromApi();
        }

        if (StringUtils.isEmpty(maxValue)) {
            throw new RuntimeException("Can't get the max value from accumulator");
        }

        ((JdbcInputSplit) inputSplit).setEndLocation(maxValue);
    }

    public String getMaxValueFromApi(){
        if(StringUtils.isEmpty(monitorUrls)){
            return null;
        }

        String url = monitorUrls;
        if (monitorUrls.startsWith("http")) {
            url = String.format("%s/jobs/%s/accumulators", monitorUrls, jobId);
        }

        /**
         * The extra 10 times is to ensure that accumulator is updated
         */
        int maxAcquireTimes = (queryTimeOut / incrementConfig.getRequestAccumulatorInterval()) + 10;

        final String[] maxValue = new String[1];
        Gson gson = new Gson();
        UrlUtil.get(url, incrementConfig.getRequestAccumulatorInterval() * 1000, maxAcquireTimes, new UrlUtil.Callback() {

            @Override
            public void call(String response) {
                Map map = gson.fromJson(response, Map.class);

                LOG.info("Accumulator data:" + gson.toJson(map));

                List<Map> userTaskAccumulators = (List<Map>) map.get("user-task-accumulators");
                for (Map accumulator : userTaskAccumulators) {
                    if (Metrics.MAX_VALUE.equals(accumulator.get("name"))) {
                        maxValue[0] = (String) accumulator.get("value");
                        break;
                    }
                }
            }

            @Override
            public boolean isReturn() {
                return StringUtils.isNotEmpty(maxValue[0]);
            }

            @Override
            public void processError(Exception e) {
                LOG.warn("", e);
            }
        });

        return maxValue[0];
    }

    /**
     * 判断增量任务是否还能继续读取数据
     * 增量任务，startLocation = endLocation且两者都不为null，返回false，其余情况返回true
     *
     * @param split 数据分片
     * @return
     */
    protected boolean canReadData(InputSplit split) {
        //只排除增量同步
        if (!incrementConfig.isIncrement() || incrementConfig.isPolling()) {
            return true;
        }

        JdbcInputSplit jdbcInputSplit = (JdbcInputSplit) split;
        if (jdbcInputSplit.getStartLocation() == null && jdbcInputSplit.getEndLocation() == null) {
            return true;
        }

        return !StringUtils.equals(jdbcInputSplit.getStartLocation(), jdbcInputSplit.getEndLocation());
    }

    /**
     * 构造查询sql
     *
     * @param inputSplit 数据切片
     * @return 构建的sql字符串
     */
    protected String buildQuerySql(InputSplit inputSplit) {
        //QuerySqlBuilder中构建的queryTemplate
        String querySql = queryTemplate;

        if (inputSplit == null) {
            LOG.warn("inputSplit = null, Executing sql is: '{}'", querySql);
            return querySql;
        }

        JdbcInputSplit jdbcInputSplit = (JdbcInputSplit) inputSplit;

        if (StringUtils.isNotEmpty(splitKey)) {
            querySql = queryTemplate.replace("${N}", String.valueOf(numPartitions)).replace("${M}", String.valueOf(indexOfSubTask));
        }

        //是否开启断点续传
        if (restoreConfig.isRestore()) {
            if (formatState == null) {
                querySql = querySql.replace(DbUtil.RESTORE_FILTER_PLACEHOLDER, StringUtils.EMPTY);

                if (incrementConfig.isIncrement()) {
                    querySql = buildIncrementSql(jdbcInputSplit, querySql);
                }
            } else {
                boolean useMaxFunc = incrementConfig.isUseMaxFunc();
                String startLocation = getLocation(restoreColumn.getType(), formatState.getState());
                if (StringUtils.isNotBlank(startLocation)) {
                    LOG.info("update startLocation, before = {}, after = {}", jdbcInputSplit.getStartLocation(), startLocation);
                    jdbcInputSplit.setStartLocation(startLocation);
                    useMaxFunc = false;
                }
                String restoreFilter = buildIncrementFilter(restoreColumn.getType(),
                        restoreColumn.getName(),
                        jdbcInputSplit.getStartLocation(),
                        jdbcInputSplit.getEndLocation(),
                        customSql,
                        useMaxFunc);

                if (StringUtils.isNotEmpty(restoreFilter)) {
                    restoreFilter = " and " + restoreFilter;
                }

                querySql = querySql.replace(DbUtil.RESTORE_FILTER_PLACEHOLDER, restoreFilter);
            }

            querySql = querySql.replace(DbUtil.INCREMENT_FILTER_PLACEHOLDER, StringUtils.EMPTY);
        } else if (incrementConfig.isIncrement()) {
            querySql = buildIncrementSql(jdbcInputSplit, querySql);
        }

        LOG.warn("Executing sql is: '{}'", querySql);

        return querySql;
    }

    /**
     * 构造增量任务查询sql
     *
     * @param jdbcInputSplit 数据切片
     * @param querySql       已经创建的查询sql
     * @return
     */
    private String buildIncrementSql(JdbcInputSplit jdbcInputSplit, String querySql) {
        String incrementFilter = buildIncrementFilter(incrementConfig.getColumnType(),
                incrementConfig.getColumnName(),
                jdbcInputSplit.getStartLocation(),
                jdbcInputSplit.getEndLocation(),
                customSql,
                incrementConfig.isUseMaxFunc());
        if (StringUtils.isNotEmpty(incrementFilter)) {
            incrementFilter = " and " + incrementFilter;
        }

        return querySql.replace(DbUtil.INCREMENT_FILTER_PLACEHOLDER, incrementFilter);
    }

    /**
     * 构建增量任务查询sql的过滤条件
     *
     * @param incrementColType 增量字段类型
     * @param incrementCol     增量字段名称
     * @param startLocation    开始位置
     * @param endLocation      结束位置
     * @param customSql        用户自定义sql
     * @param useMaxFunc       是否保存结束位置数据
     * @return
     */
    protected String buildIncrementFilter(String incrementColType, String incrementCol, String startLocation, String endLocation, String customSql, boolean useMaxFunc) {
        LOG.info("buildIncrementFilter, incrementColType = {}, incrementCol = {}, startLocation = {}, endLocation = {}, customSql = {}, useMaxFunc = {}", incrementColType, incrementCol, startLocation, endLocation, customSql, useMaxFunc);
        StringBuilder filter = new StringBuilder(128);

        if (org.apache.commons.lang.StringUtils.isNotEmpty(customSql)) {
            incrementCol = String.format("%s.%s", DbUtil.TEMPORARY_TABLE_NAME, databaseInterface.quoteColumn(incrementCol));
        } else {
            incrementCol = databaseInterface.quoteColumn(incrementCol);
        }

        String startFilter = buildStartLocationSql(incrementColType, incrementCol, startLocation, useMaxFunc);
        if (org.apache.commons.lang.StringUtils.isNotEmpty(startFilter)) {
            filter.append(startFilter);
        }

        String endFilter = buildEndLocationSql(incrementColType, incrementCol, endLocation);
        if (org.apache.commons.lang.StringUtils.isNotEmpty(endFilter)) {
            if (filter.length() > 0) {
                filter.append(" and ").append(endFilter);
            } else {
                filter.append(endFilter);
            }
        }

        return filter.toString();
    }

    /**
     * 构建起始位置sql
     *
     * @param incrementColType 增量字段类型
     * @param incrementCol     增量字段名称
     * @param startLocation    开始位置
     * @param useMaxFunc       是否保存结束位置数据
     * @return
     */
    protected String buildStartLocationSql(String incrementColType, String incrementCol, String startLocation, boolean useMaxFunc) {
        if (org.apache.commons.lang.StringUtils.isEmpty(startLocation) || DbUtil.NULL_STRING.equalsIgnoreCase(startLocation)) {
            return null;
        }

        String operator = useMaxFunc ? " >= " : " > ";

        //增量轮询，startLocation使用占位符代替
        if (incrementConfig.isPolling()) {
            return incrementCol + operator + "?";
        }

        return getLocationSql(incrementColType, incrementCol, startLocation, operator);
    }

    /**
     * 构建结束位置sql
     *
     * @param incrementColType 增量字段类型
     * @param incrementCol     增量字段名称
     * @param endLocation      结束位置
     * @return
     */
    public String buildEndLocationSql(String incrementColType, String incrementCol, String endLocation) {
        if (org.apache.commons.lang.StringUtils.isEmpty(endLocation) || DbUtil.NULL_STRING.equalsIgnoreCase(endLocation)) {
            return null;
        }

        return getLocationSql(incrementColType, incrementCol, endLocation, " < ");
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
    protected String getLocationSql(String incrementColType, String incrementCol, String location, String operator) {
        String endTimeStr;
        String endLocationSql;

        if (ColumnType.isTimeType(incrementColType)) {
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
    protected String getTimeStr(Long location, String incrementColType) {
        String timeStr;
        Timestamp ts = new Timestamp(DbUtil.getMillis(location));
        ts.setNanos(DbUtil.getNanos(location));
        timeStr = DbUtil.getNanosTimeStr(ts.toString());
        timeStr = timeStr.substring(0, 26);
        timeStr = String.format("'%s'", timeStr);

        return timeStr;
    }

    /**
     * 从数据库中查询增量字段的最大值
     *
     * @return
     */
    private String getMaxValueFromDb() {
        String maxValue = null;
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            long startTime = System.currentTimeMillis();

            String queryMaxValueSql;
            if (StringUtils.isNotEmpty(customSql)) {
                queryMaxValueSql = String.format("select max(%s.%s) as max_value from ( %s ) %s", DbUtil.TEMPORARY_TABLE_NAME,
                        databaseInterface.quoteColumn(incrementConfig.getColumnName()), customSql, DbUtil.TEMPORARY_TABLE_NAME);
            } else {
                queryMaxValueSql = String.format("select max(%s) as max_value from %s",
                        databaseInterface.quoteColumn(incrementConfig.getColumnName()), databaseInterface.quoteTable(table));
            }

            String startSql = buildStartLocationSql(incrementConfig.getColumnType(),
                    databaseInterface.quoteColumn(incrementConfig.getColumnName()),
                    incrementConfig.getStartLocation(),
                    incrementConfig.isUseMaxFunc());
            if (StringUtils.isNotEmpty(startSql)) {
                queryMaxValueSql += " where " + startSql;
            }

            LOG.info(String.format("Query max value sql is '%s'", queryMaxValueSql));

            conn = DbUtil.getConnection(dbUrl, username, password);
            st = conn.createStatement();
            rs = st.executeQuery(queryMaxValueSql);
            if (rs.next()) {
                maxValue = getLocation(incrementConfig.getColumnType(), rs.getObject("max_value"));
            }

            LOG.info(String.format("Takes [%s] milliseconds to get the maximum value [%s]", System.currentTimeMillis() - startTime, maxValue));

            return maxValue;
        } catch (Throwable e) {
            throw new RuntimeException("Get max value from " + table + " error", e);
        } finally {
            DbUtil.closeDbResources(rs, st, conn, false);
        }
    }

    /**
     * 边界位置值转字符串
     *
     * @param columnType 边界字段类型
     * @param columnVal  边界值
     * @return
     */
    private String getLocation(String columnType, Object columnVal) {
        String location;
        if (columnVal == null) {
            return null;
        }

        if (ColumnType.isTimeType(columnType)) {
            if (columnVal instanceof Long) {
                location = columnVal.toString();
            } else if (columnVal instanceof Timestamp) {
                long time = ((Timestamp) columnVal).getTime() / 1000;

                String nanosStr = String.valueOf(((Timestamp) columnVal).getNanos());
                if (nanosStr.length() == 9) {
                    location = time + nanosStr;
                } else {
                    String fillZeroStr = StringUtils.repeat("0", 9 - nanosStr.length());
                    location = time + fillZeroStr + nanosStr;
                }
            } else {
                Date date = DateUtil.stringToDate(columnVal.toString(), null);
                String fillZeroStr = StringUtils.repeat("0", 6);
                long time = date.getTime();
                location = time + fillZeroStr;
            }
        } else if (ColumnType.isNumberType(columnType)) {
            location = String.valueOf(columnVal);
        } else {
            location = String.valueOf(columnVal);
        }

        return location;
    }

    /**
     * 上传累加器数据
     *
     * @throws IOException
     */
    private void uploadMetricData() throws IOException {
        FSDataOutputStream out = null;
        try {
            org.apache.hadoop.conf.Configuration conf = FileSystemUtil.getConfiguration(hadoopConfig, null);

            Map<String, String> vars = getRuntimeContext().getMetricGroup().getAllVariables();
            String jobId = vars.get("<job_id>");
            String taskId = vars.get("<task_id>");
            String subtaskIndex = vars.get("<subtask_index>");
            LOG.info("jobId:{} taskId:{} subtaskIndex:{}", jobId, taskId, subtaskIndex);

            Path remotePath = new Path(conf.get("fs.defaultFS"), "/tmp/logs/admin/logs/" + jobId + "/" + taskId + "_" + subtaskIndex);
            FileSystem fs = FileSystemUtil.getFileSystem(hadoopConfig, null);
            out = FileSystem.create(fs, remotePath, new FsPermission(FsPermission.createImmutable((short) 0777)));

            Map<String, Object> metrics = new HashMap<>(3);
            metrics.put(Metrics.TABLE_COL, table + "-" + incrementConfig.getColumnName());
            if (startLocationAccumulator != null) {
                metrics.put(Metrics.START_LOCATION, startLocationAccumulator.getLocalValue());
            }
            if (endLocationAccumulator != null) {
                metrics.put(Metrics.END_LOCATION, endLocationAccumulator.getLocalValue());
            }
            out.writeUTF(new ObjectMapper().writeValueAsString(metrics));
        } catch (Exception e) {
            LOG.error("hadoop conf:{}", hadoopConfig);
            throw new IOException("Upload metric to HDFS error", e);
        } finally {
            IOUtils.closeStream(out);
        }
    }

    /**
     * 增量轮询查询
     *
     * @param startLocation
     * @throws SQLException
     */
    protected void queryForPolling(String startLocation) throws SQLException {
        LOG.trace("polling startLocation = {}", startLocation);
        if(StringUtils.isNotBlank(startLocation)){
            if(isTimestamp){
                ps.setTimestamp(1, Timestamp.valueOf(startLocation));
            }else{
                ps.setInt(1, Integer.parseInt(startLocation));
            }
            resultSet = ps.executeQuery();
            hasNext = resultSet.next();
        }else{
            queryStartLocation();
        }
    }

    /**
     * 执行查询
     * @param startLocation
     * @throws SQLException
     */
    protected void executeQuery(String startLocation) throws SQLException {
        if(dbConn==null){
            dbConn = DbUtil.getConnection(dbUrl, username, password);
        }
        // 部分驱动需要关闭事务自动提交，fetchSize参数才会起作用
        dbConn.setAutoCommit(false);
        if (incrementConfig.isPolling()) {
            if(StringUtils.isBlank(startLocation)){
                LOG.info("startLocation = null, execute sql = {}", querySql);
                queryStartLocation();
            }else{
                ps = dbConn.prepareStatement(querySql);
                ps.setFetchSize(fetchSize);
                ps.setQueryTimeout(queryTimeOut);
                queryForPolling(startLocation);
            }
        } else {
            Statement statement = dbConn.createStatement(resultSetType, resultSetConcurrency);
            statement.setFetchSize(fetchSize);
            statement.setQueryTimeout(queryTimeOut);
            resultSet = statement.executeQuery(querySql);
            hasNext = resultSet.next();
        }
    }

    private void queryStartLocation() throws SQLException{
        ps = dbConn.prepareStatement(querySql,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
        ps.setFetchSize(fetchSize);
        ps.setFetchDirection(ResultSet.FETCH_REVERSE);
        ps.setQueryTimeout(queryTimeOut);
        resultSet = ps.executeQuery();
        hasNext = resultSet.next();
        if(hasNext){
            querySql = querySql + "and " + databaseInterface.quoteColumn(incrementConfig.getColumnName()) + " > ?";
            ps = dbConn.prepareStatement(querySql);
            ps.setFetchSize(fetchSize);
            ps.setQueryTimeout(queryTimeOut);
            LOG.info("update querySql, sql = {}", querySql);
        }
    }

}