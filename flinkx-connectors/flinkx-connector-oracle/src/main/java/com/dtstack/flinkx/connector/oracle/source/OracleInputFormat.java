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

import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.flinkx.connector.oracle.converter.OracleRawTypeConverter;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.types.logical.RowType;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleInputFormat extends JdbcInputFormat {

    @Override
    public void openInternal(InputSplit inputSplit) {
        super.openInternal(inputSplit);
        RowType rowType =
                TableUtil.createRowType(
                        columnNameList, columnTypeList, OracleRawTypeConverter::apply);
        setRowConverter(rowConverter ==null ? jdbcDialect.getColumnConverter(rowType) : rowConverter);
    }
}
