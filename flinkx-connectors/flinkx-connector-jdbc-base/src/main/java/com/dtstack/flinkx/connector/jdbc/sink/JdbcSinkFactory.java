/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.connector.jdbc.sink;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.jdbc.adapter.ConnectionAdapter;
import com.dtstack.flinkx.connector.jdbc.conf.ConnectionConf;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.exclusion.FieldNameExclusionStrategy;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.sink.SinkFactory;
import com.dtstack.flinkx.table.options.SinkOptions;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Date: 2021/04/13 Company: www.dtstack.com
 *
 * @author tudou
 */
public abstract class JdbcSinkFactory extends SinkFactory {

    private static final int DEFAULT_CONNECTION_TIMEOUT = 600;

    protected JdbcConf jdbcConf;
    protected JdbcDialect jdbcDialect;

    public JdbcSinkFactory(SyncConf syncConf, JdbcDialect jdbcDialect) {
        super(syncConf);
        this.jdbcDialect = jdbcDialect;
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(
                                ConnectionConf.class, new ConnectionAdapter("SinkConnectionConf"))
                        .addDeserializationExclusionStrategy(
                                new FieldNameExclusionStrategy("column"))
                        .create();
        GsonUtil.setTypeAdapter(gson);
        jdbcConf = gson.fromJson(gson.toJson(syncConf.getWriter().getParameter()), JdbcConf.class);
        int batchSize =
                syncConf.getWriter()
                        .getIntVal(
                                "batchSize", SinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS.defaultValue());
        jdbcConf.setBatchSize(batchSize);
        long flushIntervalMills =
                syncConf.getWriter()
                        .getLongVal(
                                "flushIntervalMills",
                                SinkOptions.SINK_BUFFER_FLUSH_INTERVAL.defaultValue());
        jdbcConf.setFlushIntervalMills(flushIntervalMills);
        jdbcConf.setColumn(syncConf.getWriter().getFieldList());
        Properties properties = syncConf.getWriter().getProperties("properties", null);
        jdbcConf.setProperties(properties);
        if (StringUtils.isNotEmpty(syncConf.getWriter().getSemantic())) {
            jdbcConf.setSemantic(syncConf.getWriter().getSemantic());
        }
        super.initFlinkxCommonConf(jdbcConf);
        resetTableInfo();
        rebuildJdbcConf(jdbcConf);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        JdbcOutputFormatBuilder builder = getBuilder();

        int connectTimeOut = jdbcConf.getConnectTimeOut();
        jdbcConf.setConnectTimeOut(
                connectTimeOut == 0 ? DEFAULT_CONNECTION_TIMEOUT : connectTimeOut);

        builder.setJdbcConf(jdbcConf);
        builder.setJdbcDialect(jdbcDialect);

        AbstractRowConverter rowConverter = null;
        // 同步任务使用transform
        if (!useAbstractBaseColumn) {
            final RowType rowType =
                    TableUtil.createRowType(jdbcConf.getColumn(), getRawTypeConverter());
            rowConverter = jdbcDialect.getRowConverter(rowType);
        }
        builder.setRowConverter(rowConverter);

        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return jdbcDialect.getRawTypeConverter();
    }

    /**
     * 获取JDBC插件的具体outputFormatBuilder
     *
     * @return JdbcOutputFormatBuilder
     */
    protected JdbcOutputFormatBuilder getBuilder() {
        return new JdbcOutputFormatBuilder(new JdbcOutputFormat());
    }

    /** table字段有可能是schema.table格式 需要转换为对应的schema 和 table 字段* */
    protected void resetTableInfo() {
        if (StringUtils.isBlank(jdbcConf.getSchema())) {
            JdbcUtil.resetSchemaAndTable(jdbcConf, "\\\"", "\\\"");
        }
    }

    protected void rebuildJdbcConf(JdbcConf jdbcConf) {
        // updateKey has Deprecated，please use uniqueKey
        if (MapUtils.isNotEmpty(jdbcConf.getUpdateKey())
                && CollectionUtils.isEmpty(jdbcConf.getUniqueKey())) {
            for (Map.Entry<String, List<String>> entry : jdbcConf.getUpdateKey().entrySet()) {
                if (CollectionUtils.isNotEmpty(entry.getValue())) {
                    jdbcConf.setUniqueKey(entry.getValue());
                    break;
                }
            }
        }
    }
}
