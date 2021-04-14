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
package com.dtstack.flinkx.connector.mysql.outputFormat;

import com.dtstack.flinkx.connector.mysql.converter.MysqlTypeConverter;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.dtstack.flinkx.connector.jdbc.outputformat.JdbcOutputFormat;
import com.dtstack.flinkx.connector.mysql.MySQLRowConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2021/04/13
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class MysqlOutputFormat extends JdbcOutputFormat {
    protected static final Logger LOG = LoggerFactory.getLogger(MysqlOutputFormat.class);

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        super.openInternal(taskNumber, numTasks);
        try {
            RowType rowType = (RowType) MysqlTypeConverter.createRowType(column ,columnType);
            MySQLRowConverter converter = new MySQLRowConverter(rowType);
            setJdbcRowConverter(converter);
        } catch (SQLException e) {
            LOG.error("", e);
        }

    }
}
