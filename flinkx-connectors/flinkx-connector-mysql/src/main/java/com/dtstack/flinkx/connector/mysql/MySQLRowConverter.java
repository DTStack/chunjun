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

package com.dtstack.flinkx.connector.mysql;

import com.dtstack.flinkx.connector.jdbc.converter.AbstractJdbcRowConverter;

import com.dtstack.flinkx.util.DateUtil;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Locale;

/**
 * @program: luna-flink
 * @author: wuren
 * @create: 2021/03/29
 **/
public class MySQLRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    // TODO 是否需要删除
    public String converterName() {
        return "MySQL";
    }

    public MySQLRowConverter(RowType rowType) {
        super(rowType);
    }
}
