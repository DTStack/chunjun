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

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.dtstack.flinkx.connector.jdbc.outputformat.JdbcOutputFormat;
import com.dtstack.flinkx.connector.mysql.MySQLRowConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2021/04/13
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class MysqlOutputFormat extends JdbcOutputFormat {

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        super.openInternal(taskNumber, numTasks);
        List<RowType.RowField> fields = new ArrayList<>();
        for (int i = 0; i < columnType.size(); i++) {
            String name = column.get(i);
            LogicalType type;
            if (columnType.get(i).equalsIgnoreCase("BIGINT")) {
                type = new BigIntType();
            } else if (columnType.get(i).equalsIgnoreCase("VARCHAR")) {
                type = new VarCharType(40);
            } else {
                // TODO 现在就支持INT 和 STRING
                type = new VarCharType(40);
            }
            fields.add(new RowType.RowField(name, type));
        }
        RowType type = new RowType(fields);
        MySQLRowConverter converter = new MySQLRowConverter(type);
        setJdbcRowConverter(converter);
    }
}
