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
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.FileSystemUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.RetryUtil;
import com.dtstack.flinkx.util.MapUtil;
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.util.UrlUtil;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * InputFormat for reading data from a database and generate Rows.
 * <p>
 * Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class JdbcInputFormat extends BaseRichInputFormat {

    public static final long serialVersionUID = 1L;
    public static final int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    public static int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
    public DatabaseInterface databaseInterface;

    public String table;
    public String queryTemplate;
    public String customSql;
    public String querySql;

    public String splitKey;
    public int fetchSize;
    public int queryTimeOut;
    public IncrementConfig incrementConfig;

    public String username;
    public String password;
    public String driverName;
    public String dbUrl;
    public Properties properties;
    public transient Connection dbConn;
    public transient Statement statement;
    public transient PreparedStatement ps;
    public transient ResultSet resultSet;
    public boolean hasNext;


    public List<MetaColumn> metaColumns;
    public List<String> columnTypeList;
    public int columnCount;
    public MetaColumn restoreColumn;
    public Row lastRow = null;

    //for postgre
    public TypeConverterInterface typeConverter;

    public int numPartitions;

    public StringAccumulator maxValueAccumulator;
    public BigIntegerMaximum endLocationAccumulator;
    public BigIntegerMaximum startLocationAccumulator;

    //轮询增量标识字段类型
    public ColumnType type;

    //The hadoop config for metric
    public Map<String, Object> hadoopConfig;

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
        LOG.info("inputSplit = {}", inputSplit);
        ClassUtil.forName(driverName, getClass().getClassLoader());
        initMetric(inputSplit);
        if (!canReadData(inputSplit)) {
            LOG.warn("Not read data when the start location are equal to end location");
            hasNext = false;
            return;
        }
        querySql = buildQuerySql(inputSplit);
        try {
            executeQuery(((JdbcInputSplit) inputSplit).getStartLocation());
           if(!resultSet.isClosed()){
               columnCount = resultSet.getMetaData().getColumnCount();
           }
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }

        boolean splitWithRowCol = numPartitions > 1 && StringUtils.isNotEmpty(splitKey) && splitKey.contains("(");
        if (splitWithRowCol) {
            columnCount = columnCount - 1;
        }
        checkSize(columnCount, metaColumns);
        columnTypeList = DbUtil.analyzeColumnType(resultSet, metaColumns);
        LOG.info("JdbcInputFormat[{}]open: end", jobName);
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        JdbcInputSplit[] splits = new JdbcInputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new JdbcInputSplit(i, numPartitions, i, incrementConfig.getStartLocation(), null);
        }

        return splits;
    }

    @Override
    public boolean reachedEnd() throws IOException{
        if (hasNext) {
            return false;
        } else {
            if (incrementConfig.isPolling()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(incrementConfig.getPollingInterval());
                    //sqlserver、DB2驱动包不支持isValid()，这里先注释掉，后续更换驱动包
                    //间隔轮询检测数据库连接是否断开，超时时间三秒，断开后自动重连
//                    if(!dbConn.isValid(3)){
//                        dbConn = DbUtil.getConnection(dbUrl, username, password);
//                        //重新连接后还是不可用则认为数据库异常，任务失败
//                        if(!dbConn.isValid(3)){
//                            String message = String.format("cannot connect to %s, username = %s, please check %s is available.", dbUrl, username, databaseInterface.getDatabaseType());
//                            LOG.error(message);
//                            throw new RuntimeException(message);
//                        }
//                    }
                    if(!dbConn.getAutoCommit()){
                        dbConn.setAutoCommit(true);
                    }
                    DbUtil.closeDbResources(resultSet, null, null, false);
                    //此处endLocation理应不会为空
                    queryForPolling(endLocationAccumulator.getLocalValue().toString());
                    return false;
                } catch (InterruptedException e) {
                    LOG.warn("interrupted while waiting for polling, e = {}", ExceptionUtil.getErrorMessage(e));
                } catch (SQLException e) {
                    DbUtil.closeDbResources(resultSet, ps, null, false);
                    String message = String.format("error to execute sql = %s, startLocation = %s, e = %s", querySql, endLocationAccumulator.getLocalValue(), ExceptionUtil.getErrorMessage(e));
                    LOG.error(message);
                    throw new RuntimeException(message, e);
                }
            }
            return true;
        }
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        try {
            updateColumnCount();
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
                String location = null;
                Object obj = resultSet.getObject(incrementConfig.getColumnName());
                if(obj != null) {
                    if((obj instanceof java.util.Date
                            || obj.getClass().getSimpleName().toUpperCase().contains(ColumnType.TIMESTAMP.name())) ) {
                        obj = resultSet.getTimestamp(incrementConfig.getColumnName()).getTime();
                    }
                    location = String.valueOf(obj);
                    endLocationAccumulator.add(new BigInteger(location));
                }

                LOG.trace("update endLocationAccumulator, current Location = {}", location);
            }

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
     * 初始化增量或或间隔轮询任务累加器
     *
     * @param inputSplit 数据分片
     */
    protected void initMetric(InputSplit inputSplit) {
        if (!incrementConfig.isIncrement()) {
            return;
        }
        //初始化增量、轮询字段类型
        type = ColumnType.fromString(incrementConfig.getColumnType());
        startLocationAccumulator = new BigIntegerMaximum();
        endLocationAccumulator = new BigIntegerMaximum();
        String startLocation = StringUtil.stringToTimestampStr(incrementConfig.getStartLocation(), type);


        if (StringUtils.isNotEmpty(incrementConfig.getStartLocation())) {
            ((JdbcInputSplit) inputSplit).setStartLocation(startLocation);
            startLocationAccumulator.add(new BigInteger(startLocation));
        }

        //轮询任务endLocation设置为startLocation的值
        if (incrementConfig.isPolling()) {
            if (StringUtils.isNotEmpty(startLocation)) {
                endLocationAccumulator.add(new BigInteger(startLocation));
            }
        } else if (incrementConfig.isUseMaxFunc()) {
            //如果不是轮询任务，则只能是增量任务，若useMaxFunc设置为true，则去数据库查询当前增量字段的最大值
            getMaxValue(inputSplit);
            //endLocation设置为数据库中查询的最大值
            String endLocation = ((JdbcInputSplit) inputSplit).getEndLocation();
            endLocationAccumulator.add(new BigInteger(StringUtil.stringToTimestampStr(endLocation, type)));
        }else{
            //增量任务，且useMaxFunc设置为false，如果startLocation不为空，则将endLocation初始值设置为startLocation的值，防止数据库无增量数据时下次获取到的startLocation为空
            if (StringUtils.isNotEmpty(startLocation)) {
                endLocationAccumulator.add(new BigInteger(startLocation));
            }
        }

        //将累加器信息添加至prometheus
        customPrometheusReporter.registerMetric(startLocationAccumulator, Metrics.START_LOCATION);
        customPrometheusReporter.registerMetric(endLocationAccumulator, Metrics.END_LOCATION);
        getRuntimeContext().addAccumulator(Metrics.START_LOCATION, startLocationAccumulator);
        getRuntimeContext().addAccumulator(Metrics.END_LOCATION, endLocationAccumulator);
    }

    /**
     * 将增量任务的数据最大值设置到累加器中
     *
     * @param inputSplit 数据分片
     */
    protected void getMaxValue(InputSplit inputSplit) {
        String maxValue;
        if (inputSplit.getSplitNumber() == 0) {
            maxValue = getMaxValueFromDb();
            //将累加器信息上传至flink，供其他通道通过flink rest api获取该最大值
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

            conn = getConnection();
            st = conn.createStatement(resultSetType, resultSetConcurrency);
            rs = st.executeQuery(queryMaxValueSql);
            if (rs.next()) {
                switch (type) {
                    case TIMESTAMP:
                        maxValue = String.valueOf(rs.getTimestamp("max_value").getTime());
                        break;
                    case DATE:
                        maxValue = String.valueOf(rs.getDate("max_value").getTime());
                        break;
                    default:
                        maxValue = StringUtil.stringToTimestampStr(String.valueOf(rs.getObject("max_value")), type);
                }
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
     * 从flink rest api中获取累加器最大值
     * @return
     */
    @SuppressWarnings("unchecked")
    public String getMaxValueFromApi(){
        if(StringUtils.isEmpty(monitorUrls)){
            return null;
        }

        String url = monitorUrls;
        if (monitorUrls.startsWith(ConstantValue.PROTOCOL_HTTP)) {
            url = String.format("%s/jobs/%s/accumulators", monitorUrls, jobId);
        }

        //The extra 10 times is to ensure that accumulator is updated
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
                LOG.warn(ExceptionUtil.getErrorMessage(e));
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
     * 边界位置值转字符串
     *
     * @param columnType 边界字段类型
     * @param columnVal  边界值
     * @return
     */
    private String getLocation(String columnType, Object columnVal) {
        if (columnVal == null) {
            return null;
        }
        return columnVal.toString();
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
            out.writeUTF(MapUtil.writeValueAsString(metrics));
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
        boolean isNumber = StringUtils.isNumeric(startLocation);
        switch (type) {
            case TIMESTAMP:
                Timestamp ts = isNumber ? new Timestamp(Long.parseLong(startLocation)) : Timestamp.valueOf(startLocation);
                ps.setTimestamp(1, ts);
                break;
            case DATE:
                Date date = isNumber ? new Date(Long.parseLong(startLocation)) : Date.valueOf(startLocation);
                ps.setDate(1, date);
                break;
            default:
                if(isNumber){
                    ps.setLong(1, Long.parseLong(startLocation));
                }else {
                    ps.setString(1, startLocation);
                }
        }
        resultSet = ps.executeQuery();
        hasNext = resultSet.next();
    }

    /**
     * 执行查询
     * @param startLocation
     * @throws SQLException
     */
    protected void executeQuery(String startLocation) throws SQLException {
        dbConn = getConnection();
        // 部分驱动需要关闭事务自动提交，fetchSize参数才会起作用
        dbConn.setAutoCommit(false);
        if (incrementConfig.isPolling()) {
            if(StringUtils.isBlank(startLocation)){
                //从数据库中获取起始位置
                queryStartLocation();
            }else{
                ps = dbConn.prepareStatement(querySql, resultSetType, resultSetConcurrency);
                ps.setFetchSize(fetchSize);
                ps.setQueryTimeout(queryTimeOut);
                queryForPolling(startLocation);
            }
        } else {
            statement = dbConn.createStatement(resultSetType, resultSetConcurrency);
            statement.setFetchSize(fetchSize);
            statement.setQueryTimeout(queryTimeOut);
            resultSet = statement.executeQuery(querySql);
            hasNext = resultSet.next();
        }
    }

    /**
     * 间隔轮询查询起始位置
     * @throws SQLException
     */
    private void queryStartLocation() throws SQLException{
        StringBuilder builder = new StringBuilder(128);
        builder.append(querySql)
                .append("ORDER BY ")
                .append(ConstantValue.DOUBLE_QUOTE_MARK_SYMBOL)
                .append(incrementConfig.getColumnName())
                .append(ConstantValue.DOUBLE_QUOTE_MARK_SYMBOL);
        ps = dbConn.prepareStatement(builder.toString(), resultSetType, resultSetConcurrency);
        ps.setFetchSize(fetchSize);
        //第一次查询数据库中增量字段的最大值
        ps.setFetchDirection(ResultSet.FETCH_REVERSE);
        ps.setQueryTimeout(queryTimeOut);
        resultSet = ps.executeQuery();
        hasNext = resultSet.next();

        try {
            //间隔轮询一直循环，直到查询到数据库中的数据为止
            while(!hasNext){
                TimeUnit.MILLISECONDS.sleep(incrementConfig.getPollingInterval());
                //执行到此处代表轮询任务startLocation为空，且数据库中无数据，此时需要查询增量字段的最小值
                ps.setFetchDirection(ResultSet.FETCH_FORWARD);
                resultSet.close();
                //如果事务不提交 就会导致数据库即使插入数据 也无法读到数据
                dbConn.commit();
                resultSet = ps.executeQuery();
                hasNext = resultSet.next();
                //每隔五分钟打印一次，(当前时间 - 任务开始时间) % 300秒 <= 一个间隔轮询周期
                if((System.currentTimeMillis() - startTime) % 300000 <= incrementConfig.getPollingInterval()){
                    LOG.info("no record matched condition in database, execute query sql = {}, startLocation = {}", querySql, endLocationAccumulator.getLocalValue());
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("interrupted while waiting for polling, e = {}", ExceptionUtil.getErrorMessage(e));
        }

        //查询到数据，更新querySql
        builder = new StringBuilder(128);
        builder.append(querySql)
                .append("and ")
                .append(databaseInterface.quoteColumn(incrementConfig.getColumnName()))
                .append(" > ? ORDER BY \"")
                .append(incrementConfig.getColumnName())
                .append(ConstantValue.DOUBLE_QUOTE_MARK_SYMBOL);
        querySql = builder.toString();
        ps = dbConn.prepareStatement(querySql, resultSetType, resultSetConcurrency);
        ps.setFetchDirection(ResultSet.FETCH_REVERSE);
        ps.setFetchSize(fetchSize);
        ps.setQueryTimeout(queryTimeOut);
        LOG.info("update querySql, sql = {}", querySql);
    }

    /**
     * 获取数据库连接，用于子类覆盖
     * @return connection
     */
    protected Connection getConnection() throws SQLException {
        return RetryUtil.executeWithRetry(() -> DriverManager.getConnection(dbUrl, username, password), 3, 2000,false);
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
    protected boolean makeTaskFailedWhenReportFailed() {
        return true;
    }

    /**
     * 校验columnCount和metaColumns的长度是否相等
     * @param columnCount
     * @param metaColumns
     */
    protected void checkSize(int columnCount, List<MetaColumn> metaColumns) {
        if (!ConstantValue.STAR_SYMBOL.equals(metaColumns.get(0).getName()) && columnCount != metaColumns.size()) {
            String message = String.format("error config: column = %s, column size = %s, but columns size for query result is %s. And the query sql is %s.",
                    GsonUtil.GSON.toJson(metaColumns),
                    metaColumns.size(),
                    columnCount,
                    querySql);
            LOG.error(message);
            throw new RuntimeException(message);
        }
    }

    /**
     * 兼容db2 在间隔轮训场景 且第一次读取时没有任何数据
     * 在openInternal方法调用时 由于数据库没有数据，db2会自动关闭resultSet，因此只有在间隔轮训中某次读取到数据之后，进行更新columnCount
     * @throws SQLException
     */
    private  void updateColumnCount() throws SQLException {
        if(columnCount == 0){
            columnCount =resultSet.getMetaData().getColumnCount();
            boolean splitWithRowCol = numPartitions > 1 && StringUtils.isNotEmpty(splitKey) && splitKey.contains("(");
            if (splitWithRowCol) {
                columnCount = columnCount - 1;
            }
        }
    }
}