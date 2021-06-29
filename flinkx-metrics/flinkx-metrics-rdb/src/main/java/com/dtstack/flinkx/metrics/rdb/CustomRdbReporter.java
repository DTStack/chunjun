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

package com.dtstack.flinkx.metrics.rdb;

import com.dtstack.flinkx.conf.MetricParam;
import com.dtstack.flinkx.metrics.CustomReporter;
import com.dtstack.flinkx.util.JsonUtil;
import com.google.common.collect.Maps;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.runtime.metrics.groups.ReporterScopedSettings;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
public abstract class CustomRdbReporter extends CustomReporter {


    protected JdbcMetricConf jdbcMetricConf;

    protected JdbcDialect jdbcDialect;

    protected transient Connection dbConn;


    private List<String> fields = Arrays.asList("job_id", "job_name", "task_id", "task_name", "subtask_index");

    private static List<String> columns = Arrays.asList("jobId",
            "jobName",
            "taskId",
            "taskName",
            "taskIndex",
            "metricName",
            "value");


    private Map<String, List<String>> metricDimensionValues = Maps.newConcurrentMap();

    private Map<String, Accumulator> accumulatorMap = Maps.newConcurrentMap();

    public CustomRdbReporter(MetricParam metricParam) {
        super(metricParam);
        jdbcMetricConf = JsonUtil.toObject(
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


    //如何兼容多并行度的情况下的指标写入,TODO  暂时不考虑
    @Override
    public void registerMetric(Accumulator accumulator, String name) {
        accumulatorMap.putIfAbsent(name, accumulator);
        ReporterScopedSettings reporterScopedSettings = new ReporterScopedSettings(0, ',', Collections.emptySet());
        FrontMetricGroup front = new FrontMetricGroup<AbstractMetricGroup<?>>(
                reporterScopedSettings,
                (AbstractMetricGroup) context.getMetricGroup());
        List<String> singleValues = new ArrayList<>();
        Map<String, String> metricMap = front.getAllVariables();
        Map<String, String> metricFilterMap = Maps.newHashMap();
        for (final Map.Entry<String, String> entry : metricMap.entrySet()) {
            String newKey = CHARACTER_FILTER.filterCharacters(entry.getKey().substring(1, entry.getKey().length() - 1));
            String newValue = labelValueCharactersFilter.filterCharacters(entry.getValue());
            metricFilterMap.putIfAbsent(newKey, newValue);
        }
        fields.forEach(field -> {
            singleValues.add(metricFilterMap.get(field));
        });
        metricDimensionValues.putIfAbsent(name, singleValues);
    }

    /**
     * create metric table
     */
    public abstract void createTableIfNotExist();

    @Override
    public void report() {
        try (PreparedStatement ps = dbConn.prepareStatement(prepareTemplates())) {
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
        }
    }


    protected String prepareTemplates() {
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
