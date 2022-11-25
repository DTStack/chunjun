/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.connector.influxdb.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.influxdb.config.InfluxdbSourceConfig;
import com.dtstack.chunjun.constants.ConstantValue;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

public class InfluxdbQuerySqlBuilder {
    protected static final String CUSTOM_INFLUXQL_TEMPLATE = "select * from (%s)";

    protected String measurement;
    protected List<FieldConfig> fieldConfList;
    protected String splitKey;
    protected String customFilter;
    protected String customInfluxql;
    protected boolean isSplitByKey;
    protected List<String> fieldList;

    public InfluxdbQuerySqlBuilder(InfluxdbSourceConfig influxDBConfig, List<String> fieldList) {
        this.measurement = influxDBConfig.getMeasurement();
        this.splitKey = influxDBConfig.getSplitPk();
        this.customFilter = influxDBConfig.getWhere();
        this.isSplitByKey =
                influxDBConfig.getParallelism() > 1
                        && StringUtils.isNotBlank(influxDBConfig.getSplitPk());
        this.fieldConfList = influxDBConfig.getColumn();
        this.fieldList = fieldList;
    }

    public String buildSql() {
        String query;
        if (StringUtils.isNotEmpty(customInfluxql)) {
            query = buildQuerySqlWithCustomInfluxql();
        } else {
            query = buildQueryInfluxql();
        }

        return query;
    }

    protected String buildQueryInfluxql() {
        List<String> selectColumns;
        if (!(fieldConfList.size() == 1
                && StringUtils.equals(ConstantValue.STAR_SYMBOL, fieldConfList.get(0).getName()))) {
            selectColumns =
                    fieldConfList.stream()
                            .filter(e -> fieldList.contains(e.getName()))
                            .map(e -> quota(e.getName()))
                            .collect(Collectors.toList());
        } else {
            selectColumns =
                    fieldList.stream()
                            .map(InfluxdbQuerySqlBuilder::quota)
                            .collect(Collectors.toList());
        }
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(StringUtils.join(selectColumns, ",")).append(" FROM ");
        sb.append(quota(measurement));
        sb.append(" WHERE 1=1 ");
        StringBuilder filter = new StringBuilder();
        if (isSplitByKey) {
            filter.append(" AND ").append(String.format("%s%%${N} = ${M}", quota(splitKey)));
        }
        if (customFilter != null) {
            customFilter = customFilter.trim();
            if (customFilter.length() > 0) {
                filter.append(" AND ").append(customFilter);
            }
        }
        sb.append(filter);
        sb.append(" order by \"time\"");
        return sb.toString();
    }

    protected String buildQuerySqlWithCustomInfluxql() {
        StringBuilder querySql = new StringBuilder();
        querySql.append(String.format(CUSTOM_INFLUXQL_TEMPLATE, customInfluxql));
        querySql.append(" WHERE 1=1 ");

        if (isSplitByKey) {
            querySql.append(" AND ").append(String.format("%s%%${N} = ${M}", quota(splitKey)));
        }

        if (customFilter != null) {
            customFilter = customFilter.trim();
            if (customFilter.length() > 0) {
                querySql.append(" AND ").append(customFilter);
            }
        }

        return querySql.toString();
    }

    public static String quota(String col) {
        return "\"" + col + "\"";
    }
}
