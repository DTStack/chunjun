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
package com.dtstack.chunjun.connector.db2.table;

import com.dtstack.chunjun.connector.db2.dialect.Db2Dialect;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.table.JdbcDynamicTableFactory;

public class Db2DynamicTableFactory extends JdbcDynamicTableFactory {

    private static final String IDENTIFIER = "db2-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    protected JdbcDialect getDialect() {
        return new Db2Dialect();
    }

    @Override
    protected JdbcInputFormatBuilder getInputFormatBuilder() {
        return new JdbcInputFormatBuilder(new JdbcInputFormat());
    }

    @Override
    protected JdbcOutputFormatBuilder getOutputFormatBuilder() {
        return new JdbcOutputFormatBuilder(new JdbcOutputFormat());
    }
}
