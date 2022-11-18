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

package com.dtstack.chunjun.metrics.rdb;

import com.dtstack.chunjun.config.MetricParam;
import com.dtstack.chunjun.metrics.CustomReporter;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.metrics.filter.MetricFilter;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.runtime.metrics.groups.ReporterScopedSettings;

import com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author: shifang
 * @description rdb report
 * @date: 2021/6/28 下午5:09
 */
public abstract class RdbReporter extends CustomReporter {

    private static final char SCOPE_SEPARATOR = '_';
    protected JdbcMetricConf jdbcMetricConf;
    protected JdbcDialect jdbcDialect;
    protected transient Connection dbConn;
    protected List<String> fields =
            Arrays.asList("job_id", "job_name", "task_id", "task_name", "subtask_index");
    private Map<String, List<String>> metricDimensionValues = Maps.newConcurrentMap();
    private Map<String, Accumulator> accumulatorMap = Maps.newConcurrentMap();

    public RdbReporter(MetricParam metricParam) {
        super(metricParam);
        jdbcMetricConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(metricParam.getMetricPluginConf()), JdbcMetricConf.class);
    }

    @Override
    public void close() {
        try {
            if (dbConn != null) {
                dbConn.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void registerMetric(Accumulator accumulator, String name) {
        if (accumulatorMap.get(name) != null) {
            return;
        }
        accumulatorMap.put(name, accumulator);
        ReporterScopedSettings reporterScopedSettings =
                new ReporterScopedSettings(
                        0,
                        ',',
                        MetricFilter.NO_OP_FILTER,
                        Collections.emptySet(),
                        Collections.emptyMap());
        FrontMetricGroup front =
                new FrontMetricGroup<AbstractMetricGroup<?>>(
                        reporterScopedSettings, (AbstractMetricGroup) context.getMetricGroup());
        List<String> singleValues = new ArrayList<>();
        Map<String, String> metricMap = front.getAllVariables();
        Map<String, String> metricFilterMap = Maps.newHashMap();
        for (final Map.Entry<String, String> entry : metricMap.entrySet()) {
            String newKey =
                    CHARACTER_FILTER.filterCharacters(
                            entry.getKey().substring(1, entry.getKey().length() - 1));
            String newValue = labelValueCharactersFilter.filterCharacters(entry.getValue());
            metricFilterMap.putIfAbsent(newKey, newValue);
        }
        fields.forEach(
                field -> {
                    singleValues.add(metricFilterMap.get(field));
                });
        metricDimensionValues.putIfAbsent(name, singleValues);
    }

    /** create metric table */
    public void createTableIfNotExist() {
        try (Statement statement = dbConn.createStatement()) {
            statement.execute(
                    jdbcDialect.getCreateStatement(
                            jdbcMetricConf.getSchema(), jdbcMetricConf.getTable()));
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void report() {
        PreparedStatement ps = null;
        try {
            if (!dbConn.isValid(10)) {
                dbConn = JdbcUtil.getConnection(jdbcMetricConf, jdbcDialect);
            }
            ps = dbConn.prepareStatement(prepareTemplates());
            dbConn.setAutoCommit(false);
            for (final Map.Entry<String, Accumulator> entry : accumulatorMap.entrySet()) {
                List<String> dimensionValue = metricDimensionValues.get(entry.getKey());
                int columnIndex = 1;
                for (String value : dimensionValue) {
                    ps.setString(columnIndex, value);
                    columnIndex++;
                }
                ps.setString(columnIndex, entry.getKey());
                columnIndex++;
                ps.setString(columnIndex, entry.getValue().getLocalValue().toString());
                ps.addBatch();
            }
            ps.executeBatch();
            JdbcUtil.commit(dbConn);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            closeResource(ps);
        }
    }

    private void closeResource(AutoCloseable... closeables) {
        if (closeables == null) {
            return;
        }
        List<AutoCloseable> closeableList = Arrays.asList(closeables);
        try {
            for (AutoCloseable closeable : closeableList) {
                if (closeable != null) {
                    closeable.close();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected String prepareTemplates() {
        List<String> columns = new ArrayList(fields);
        columns.add("metric_name");
        columns.add("metric_value");
        String singleSql =
                jdbcDialect.getInsertIntoStatement(
                        jdbcMetricConf.getSchema(),
                        jdbcMetricConf.getTable(),
                        columns.toArray(new String[0]));

        return singleSql;
    }

    @Override
    public void open() {
        dbConn = JdbcUtil.getConnection(jdbcMetricConf, jdbcDialect);
        createTableIfNotExist();
    }
}
