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

package com.dtstack.flinkx.connector.mysql.table;

import com.dtstack.flinkx.connector.jdbc.table.DtJdbcDynamicTableFactory;
import com.dtstack.flinkx.connector.mysql.MySQLDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/03/17
 **/
public class DtMysqlDynamicTableFactory extends DtJdbcDynamicTableFactory {

    public static final String IDENTIFIER = "dt-mysql";
    public static final String DRIVER = "com.mysql.cj.jdbc.Driver";

    @Override
    protected String getDriver() {
        return DRIVER;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    protected JdbcDialect getDialect() {
        return new MySQLDialect();
    }

}
