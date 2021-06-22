/*
 *    Copyright 2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.dtstack.flinkx.connector.api;

import com.dtstack.flinkx.conf.FlinkxCommonConf;

import org.apache.commons.compress.utils.Lists;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DatabaseBaseRichInputFormat<T, OUT extends RowData, RAW> extends BaseRichInputFormat {

    private final static Logger LOG = LoggerFactory.getLogger(DatabaseBaseRichInputFormat.class);

    private JdbcDialect jdbcDialect;
    private List<String> column;
    private ServiceProcessor<T, OUT> processor;
    private ServiceProcessor.Context context;
    private Map<String, Object> params;

    public DatabaseBaseRichInputFormat(Map<String, Object> config) {
        this.jdbcDialect = buildJdbcDialect(config);
        this.column = buildColumns(config);
        this.params = config;
        this.config = buildFlinkxConf(config);
    }

    @SuppressWarnings("all")
    private FlinkxCommonConf buildFlinkxConf(Map<String, Object> config) {
        FlinkxCommonConf conf = new FlinkxCommonConf();
        conf.setCheckFormat((Boolean) config.getOrDefault("checkFormat", true));
        conf.setDirtyDataHadoopConf((Map<String, Object>) config.getOrDefault("dirtyDataHadoopConf", new HashMap<>()));
        conf.setDirtyDataPath((String) config.getOrDefault("dirtyDataPath", null));
        conf.setErrorPercentage((Integer) config.getOrDefault("errorPercentage", -1));
        conf.setErrorRecord((Integer) config.getOrDefault("errorRecord", 0));
        conf.setFieldNameList((List<String>) config.getOrDefault("fieldNameList", Lists.newArrayList()));
        conf.setParallelism((Integer) config.getOrDefault("parallelism", 1));
        conf.setSpeedBytes((Long) config.getOrDefault("speedBytes", 100L));
        return conf;
    }

    @SuppressWarnings("all")
    private List<String> buildColumns(Map<String, Object> config) {
        return (List<String>) config.getOrDefault("column", Lists.newArrayList());
    }

    private JdbcDialect buildJdbcDialect(Map<String, Object> config) {
        return JdbcDialects.get(String.valueOf(config.getOrDefault("jdbcUrl", "jdbc:postgresql:"))).orElseThrow(
                IllegalArgumentException::new);
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        return new SimpleInputSplit[] {new SimpleInputSplit(minNumSplits)};
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("split number = {}", inputSplit.getSplitNumber());
        this.processor = buildProcessor(params);
        context = new SimpleContext();
        context.set("split.number", inputSplit.getSplitNumber());
        LOG.info("InputFormat[{}]open: end", jobName);
    }

    @SuppressWarnings("all")
    private ServiceProcessor<T, OUT> buildProcessor(Map<String, Object> params) {
        List<ServiceProcessor> processorList = StreamSupport
                .stream(ServiceLoader.load(ServiceProcessor.class).spliterator(), false)
                .collect(Collectors.toList());
        for (ServiceProcessor processor : processorList) {
            if (isSupport(processor.type(), params)) {
                processor.setParams(params);
                return processor;
            }
        }
        return null;
    }

    private boolean isSupport(String type, Map<String, Object> params) {
        return type.equals(params.getOrDefault("processor.type", "debezium-pg"));
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws IOException {
        return rowData;
    }

    @Override
    protected void closeInternal() throws IOException {
        processor.close();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !processor.dataProcessor().moreData();
    }

//    protected void initMetric(InputSplit inputSplit) {
//        if (!jdbcConf.isIncrement()) {
//            return;
//        }
//        ColumnType type = ColumnType.fromString(jdbcConf.getIncreColumnType());
//        BigIntegerAccmulator startLocationAccumulator = new BigIntegerAccmulator();
//        BigIntegerAccmulator endLocationAccumulator = new BigIntegerAccmulator();
//        String startLocation = StringUtil.stringToTimestampStr(jdbcConf.getStartLocation(), type);
//
//        if (StringUtils.isNotEmpty(jdbcConf.getStartLocation())) {
//            ((JdbcInputSplit) inputSplit).setStartLocation(startLocation);
//            startLocationAccumulator.add(new BigInteger(startLocation));
//        }
//
//        //轮询任务endLocation设置为startLocation的值
//        if (jdbcConf.isPolling()) {
//            if (StringUtils.isNotEmpty(startLocation)) {
//                endLocationAccumulator.add(new BigInteger(startLocation));
//            }
//        } else if (jdbcConf.isUseMaxFunc()) {
//            //如果不是轮询任务，则只能是增量任务，若useMaxFunc设置为true，则去数据库查询当前增量字段的最大值
//            getMaxValue(inputSplit);
//            //endLocation设置为数据库中查询的最大值
//            String endLocation = ((JdbcInputSplit) inputSplit).getEndLocation();
//            endLocationAccumulator.add(new BigInteger(StringUtil.stringToTimestampStr(endLocation, type)));
//        } else {
//            //增量任务，且useMaxFunc设置为false，如果startLocation不为空，则将endLocation初始值设置为startLocation的值，防止数据库无增量数据时下次获取到的startLocation为空
//            if (StringUtils.isNotEmpty(startLocation)) {
//                endLocationAccumulator.add(new BigInteger(startLocation));
//            }
//        }
//
//        //将累加器信息添加至prometheus
//        customPrometheusReporter.registerMetric(startLocationAccumulator, Metrics.START_LOCATION);
//        customPrometheusReporter.registerMetric(endLocationAccumulator, Metrics.END_LOCATION);
//        getRuntimeContext().addAccumulator(Metrics.START_LOCATION, startLocationAccumulator);
//        getRuntimeContext().addAccumulator(Metrics.END_LOCATION, endLocationAccumulator);
//    }

    /**
     * 将增量任务的数据最大值设置到累加器中
     *
     * @param inputSplit 数据分片
     */
//    protected void getMaxValue(InputSplit inputSplit) {
//        String maxValue;
//        if (inputSplit.getSplitNumber() == 0) {
//            maxValue = getMaxValueFromDb();
//            //将累加器信息上传至flink，供其他通道通过flink rest api获取该最大值
//            StringAccumulator maxValueAccumulator = new StringAccumulator();
//            maxValueAccumulator.add(maxValue);
//            getRuntimeContext().addAccumulator(Metrics.MAX_VALUE, maxValueAccumulator);
//        } else {
//            maxValue = String.valueOf(accumulatorCollector.getAccumulatorValue(Metrics.MAX_VALUE, true));
//        }
//
//        ((JdbcInputSplit) inputSplit).setEndLocation(maxValue);
//    }

    /**
     * 使用自定义的指标输出器把增量指标打到普罗米修斯
     */
    @Override
    protected boolean useCustomPrometheusReporter() {
        return (boolean) params.getOrDefault("enable.prometheus", false);
    }

    /**
     * 为了保证增量数据的准确性，指标输出失败时使任务失败
     */
    protected boolean makeTaskFailedWhenReportFailed() {
        return true;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
}
