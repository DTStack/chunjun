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

package com.dtstack.flinkx.connector.oracle.source;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.flinkx.connector.oracle.OracleDialect;
import com.dtstack.flinkx.connector.oracle.converter.OracleRawTypeConverter;
import com.dtstack.flinkx.connector.oracle.util.OracleUtil;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple4;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleInputFormat extends JdbcInputFormat {

    protected static final Logger LOG = LoggerFactory.getLogger(OracleInputFormat.class);


    @Override
    public void openInternal(InputSplit inputSplit) {
        // 使 oracle 驱动按照 jdbc 规范返回数据
        System.getProperties().setProperty("oracle.jdbc.J2EE13Compliant", "true");
        super.openInternal(inputSplit);
        // 若是同步任务，则 rowConverter 为空。
        // sql 任务中 rowConverter 会在 OracleDynamicTableFactory#createDynamicTableSource() 处初始化 rowConverter
        if(rowConverter == null){
            // 说明是同步任务
            try {
                LogicalType rowType =
                        TableUtil.createRowType(
                                column, columnType, OracleRawTypeConverter::apply);
                setRowConverter(jdbcDialect.getColumnConverter((RowType) rowType));
            } catch (SQLException e) {
                LOG.error("", e);
            }
        }
    }



}
