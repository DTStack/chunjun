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
package com.dtstack.chunjun.connector.jdbc.sink;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.enums.Semantic;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class JdbcOutputFormatBuilder extends BaseRichOutputFormatBuilder<JdbcOutputFormat> {

    public JdbcOutputFormatBuilder(JdbcOutputFormat format) {
        super(format);
    }

    public void setJdbcConf(JdbcConfig jdbcConfig) {
        super.setConfig(jdbcConfig);
        format.setJdbcConf(jdbcConfig);
    }

    public void setJdbcDialect(JdbcDialect JdbcDialect) {
        format.setJdbcDialect(JdbcDialect);
    }

    @Override
    public void setRowConverter(AbstractRowConverter rowConverter) {
        format.setRowConverter(rowConverter);
    }

    public void setKeyRowType(RowType keyRowType) {
        format.setKeyRowType(keyRowType);
    }

    public void setKeyRowConverter(AbstractRowConverter keyRowConverter) {
        format.setKeyRowConverter(keyRowConverter);
    }

    public void setColumnNameList(List<String> columnNameList) {
        format.setColumnNameList(columnNameList);
    }

    public void setColumnTypeList(List<TypeConfig> columnTypeList) {
        format.setColumnTypeList(columnTypeList);
    }

    @Override
    protected void checkFormat() {
        JdbcConfig jdbcConfig = format.getJdbcConfig();
        StringBuilder sb = new StringBuilder(256);
        if (StringUtils.isBlank(jdbcConfig.getUsername())) {
            sb.append("No username supplied;\n");
        }

        if (StringUtils.isBlank(jdbcConfig.getJdbcUrl())) {
            sb.append("No jdbc url supplied;\n");
        }

        if (Semantic.getByName(jdbcConfig.getSemantic()) == Semantic.EXACTLY_ONCE
                && jdbcConfig.isAutoCommit()) {
            sb.append(
                    "Exactly-once semantics requires that the jdbc driver is not in auto-commit mode;\n");
        }

        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
