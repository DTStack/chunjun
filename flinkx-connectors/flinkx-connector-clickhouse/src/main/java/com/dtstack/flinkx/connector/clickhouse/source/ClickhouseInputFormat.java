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

package com.dtstack.flinkx.connector.clickhouse.source;

import com.dtstack.flinkx.connector.clickhouse.converter.ClickhouseRawTypeConverter;
import com.dtstack.flinkx.connector.clickhouse.util.ClickhouseUtil;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;


/**
 * @program: flinkx
 * @author: xiuzhu
 * @create: 2021/05/10
 */

public class ClickhouseInputFormat extends JdbcInputFormat {

    protected static final Logger LOG = LoggerFactory.getLogger(ClickhouseInputFormat.class);

    @Override
    public void openInternal(InputSplit inputSplit) {
        super.openInternal(inputSplit);
        try {
            LogicalType rowType =
                    TableUtil.createRowType(
                            column, columnType, ClickhouseRawTypeConverter::apply);
            setRowConverter(jdbcDialect.getColumnConverter((RowType) rowType));
        } catch (SQLException e) {
            LOG.error("", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Connection getConnection() {
        try {
            return ClickhouseUtil.getConnection(
                    jdbcConf.getJdbcUrl(),
                    jdbcConf.getUsername(),
                    jdbcConf.getPassword());
        } catch (SQLException e) {
            LOG.error("", e);
            throw new RuntimeException(e);
        }
    }
}
