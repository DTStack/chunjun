package com.dtstack.chunjun.connector.api;

import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputSplit;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.metrics.BigIntegerAccmulator;
import com.dtstack.chunjun.metrics.StringAccumulator;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.StringUtil;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class DatabaseBaseRichInputFormat<T, OUT extends RowData> extends BaseRichInputFormat {

    protected static final int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    protected static int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
    protected JdbcConf jdbcConf;
    protected JdbcDialect jdbcDialect;
    protected boolean hasNext;
    /** 用户脚本中填写的字段名称集合 */
    protected List<String> column = new ArrayList<>();
    /** 用户脚本中填写的字段类型集合 */
    protected StringAccumulator maxValueAccumulator;

    protected BigIntegerAccmulator endLocationAccumulator;
    protected BigIntegerAccmulator startLocationAccumulator;
    // 轮询增量标识字段类型
    protected ColumnType type;
    private ServiceProcessor<T, OUT> processor;
    private ServiceProcessor.Context context;

    private BlockingQueue<RowData> queue;

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        return new InputSplit[0];
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("inputSplit = {}", inputSplit);
        initMetric(inputSplit);
        if (!canReadData(inputSplit)) {
            LOG.warn("Not read data when the start location are equal to end location");
            hasNext = false;
            return;
        }

        context = new SimpleContext();
        context.set("split.number", inputSplit.getSplitNumber());
        try {
            processor.process(context);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        LOG.info("JdbcInputFormat[{}]open: end", jobName);
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        if (queue.isEmpty()) {
            ServiceProcessor.Context context = new SimpleContext();
            try {
                queue.addAll(processor.dataProcessor().process(context));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return queue.poll();
    }

    @Override
    protected void closeInternal() throws IOException {
        processor.close();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return processor.dataProcessor().moreData();
    }

    protected void initMetric(InputSplit inputSplit) {
        if (!jdbcConf.isIncrement()) {
            return;
        }
        // 初始化增量、轮询字段类型
        type = ColumnType.fromString(jdbcConf.getIncreColumnType());
        startLocationAccumulator = new BigIntegerAccmulator();
        endLocationAccumulator = new BigIntegerAccmulator();
        String startLocation = StringUtil.stringToTimestampStr(jdbcConf.getStartLocation(), type);

        if (StringUtils.isNotEmpty(jdbcConf.getStartLocation())) {
            ((JdbcInputSplit) inputSplit).setStartLocation(startLocation);
            startLocationAccumulator.add(new BigInteger(startLocation));
        }

        // 轮询任务endLocation设置为startLocation的值
        if (jdbcConf.isPolling()) {
            if (StringUtils.isNotEmpty(startLocation)) {
                endLocationAccumulator.add(new BigInteger(startLocation));
            }
        } else if (jdbcConf.isUseMaxFunc()) {
            // 如果不是轮询任务，则只能是增量任务，若useMaxFunc设置为true，则去数据库查询当前增量字段的最大值
            getMaxValue(inputSplit);
            // endLocation设置为数据库中查询的最大值
            String endLocation = ((JdbcInputSplit) inputSplit).getEndLocation();
            endLocationAccumulator.add(
                    new BigInteger(StringUtil.stringToTimestampStr(endLocation, type)));
        } else {
            // 增量任务，且useMaxFunc设置为false，如果startLocation不为空，则将endLocation初始值设置为startLocation的值，防止数据库无增量数据时下次获取到的startLocation为空
            if (StringUtils.isNotEmpty(startLocation)) {
                endLocationAccumulator.add(new BigInteger(startLocation));
            }
        }

        // 将累加器信息添加至prometheus
        //        customPrometheusReporter.registerMetric(startLocationAccumulator,
        // Metrics.START_LOCATION);
        //        customPrometheusReporter.registerMetric(endLocationAccumulator,
        // Metrics.END_LOCATION);
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
            // 将累加器信息上传至flink，供其他通道通过flink rest api获取该最大值
            maxValueAccumulator = new StringAccumulator();
            maxValueAccumulator.add(maxValue);
            getRuntimeContext().addAccumulator(Metrics.MAX_VALUE, maxValueAccumulator);
        } else {
            maxValue =
                    String.valueOf(
                            accumulatorCollector.getAccumulatorValue(Metrics.MAX_VALUE, true));
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
            if (StringUtils.isNotEmpty(jdbcConf.getCustomSql())) {
                queryMaxValueSql =
                        String.format(
                                "select max(%s.%s) as max_value from ( %s ) %s",
                                JdbcUtil.TEMPORARY_TABLE_NAME,
                                jdbcDialect.quoteIdentifier(jdbcConf.getIncreColumn()),
                                jdbcConf.getCustomSql(),
                                JdbcUtil.TEMPORARY_TABLE_NAME);
            } else {
                queryMaxValueSql =
                        String.format(
                                "select max(%s) as max_value from %s",
                                jdbcDialect.quoteIdentifier(jdbcConf.getIncreColumn()),
                                jdbcDialect.quoteIdentifier(jdbcConf.getTable()));
            }

            String startSql =
                    buildStartLocationSql(
                            jdbcConf.getIncreColumnType(),
                            jdbcDialect.quoteIdentifier(jdbcConf.getIncreColumn()),
                            jdbcConf.getStartLocation(),
                            jdbcConf.isUseMaxFunc());
            if (StringUtils.isNotEmpty(startSql)) {
                queryMaxValueSql += " where " + startSql;
            }

            LOG.info(String.format("Query max value sql is '%s'", queryMaxValueSql));

            conn = getConnection();
            st = conn.createStatement(resultSetType, resultSetConcurrency);
            st.setQueryTimeout(jdbcConf.getQueryTimeOut());
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
                        maxValue =
                                StringUtil.stringToTimestampStr(
                                        String.valueOf(rs.getObject("max_value")), type);
                }
            }

            LOG.info(
                    String.format(
                            "Takes [%s] milliseconds to get the maximum value [%s]",
                            System.currentTimeMillis() - startTime, maxValue));

            return maxValue;
        } catch (Throwable e) {
            throw new RuntimeException("Get max value from " + jdbcConf.getTable() + " error", e);
        } finally {
            JdbcUtil.closeDbResources(rs, st, conn, false);
        }
    }

    /**
     * 判断增量任务是否还能继续读取数据 增量任务，startLocation = endLocation且两者都不为null，返回false，其余情况返回true
     *
     * @param split 数据分片
     * @return
     */
    protected boolean canReadData(InputSplit split) {
        // 只排除增量同步
        if (!jdbcConf.isIncrement() || jdbcConf.isPolling()) {
            return true;
        }

        JdbcInputSplit jdbcInputSplit = (JdbcInputSplit) split;
        if (jdbcInputSplit.getStartLocation() == null && jdbcInputSplit.getEndLocation() == null) {
            return true;
        }

        return !StringUtils.equals(
                jdbcInputSplit.getStartLocation(), jdbcInputSplit.getEndLocation());
    }

    /**
     * 构建起始位置sql
     *
     * @param incrementColType 增量字段类型
     * @param incrementCol 增量字段名称
     * @param startLocation 开始位置
     * @param useMaxFunc 是否保存结束位置数据
     * @return
     */
    protected String buildStartLocationSql(
            String incrementColType,
            String incrementCol,
            String startLocation,
            boolean useMaxFunc) {
        if (org.apache.commons.lang.StringUtils.isEmpty(startLocation)
                || JdbcUtil.NULL_STRING.equalsIgnoreCase(startLocation)) {
            return null;
        }

        String operator = useMaxFunc ? " >= " : " > ";

        // 增量轮询，startLocation使用占位符代替
        if (jdbcConf.isPolling()) {
            return incrementCol + operator + "?";
        }

        return getLocationSql(incrementColType, incrementCol, startLocation, operator);
    }

    /**
     * 构建边界位置sql
     *
     * @param incrementColType 增量字段类型
     * @param incrementCol 增量字段名称
     * @param location 边界位置(起始/结束)
     * @param operator 判断符( >, >=, <)
     * @return
     */
    protected String getLocationSql(
            String incrementColType, String incrementCol, String location, String operator) {
        String endTimeStr;
        String endLocationSql;

        if (ColumnType.isTimeType(incrementColType)) {
            endTimeStr = getTimeStr(Long.parseLong(location));
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
     * @param location 边界位置(起始/结束)
     * @return
     */
    protected String getTimeStr(Long location) {
        String timeStr;
        Timestamp ts = new Timestamp(JdbcUtil.getMillis(location));
        ts.setNanos(JdbcUtil.getNanos(location));
        timeStr = JdbcUtil.getNanosTimeStr(ts.toString());
        timeStr = timeStr.substring(0, 26);
        timeStr = String.format("'%s'", timeStr);

        return timeStr;
    }

    /**
     * 获取数据库连接，用于子类覆盖
     *
     * @return connection
     */
    protected Connection getConnection() {
        return JdbcUtil.getConnection(jdbcConf, jdbcDialect);
    }

    /** 使用自定义的指标输出器把增量指标打到普罗米修斯 */
    protected boolean useCustomPrometheusReporter() {
        return jdbcConf.isIncrement();
    }

    /** 为了保证增量数据的准确性，指标输出失败时使任务失败 */
    @Override
    protected boolean makeTaskFailedWhenReportFailed() {
        return true;
    }
}
