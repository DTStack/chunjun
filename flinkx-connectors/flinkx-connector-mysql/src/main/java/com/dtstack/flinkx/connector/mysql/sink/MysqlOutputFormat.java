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

package com.dtstack.flinkx.connector.mysql.sink;

import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.flinkx.connector.mysql.converter.MysqlRawTypeConverter;
import com.dtstack.flinkx.util.TableUtil;

/**
 * Date: 2021/04/13 Company: www.dtstack.com
 *
 * @author tudou
 */
public class MysqlOutputFormat extends JdbcOutputFormat {

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        super.openInternal(taskNumber, numTasks);
        RowType rowType = TableUtil.createRowType(columnNameList, columnTypeList, MysqlRawTypeConverter::apply);
        setRowConverter(rowConverter ==null ? jdbcDialect.getColumnConverter(rowType) : rowConverter);
    }
}
