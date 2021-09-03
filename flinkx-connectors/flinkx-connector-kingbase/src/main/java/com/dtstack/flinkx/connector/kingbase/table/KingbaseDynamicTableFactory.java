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
package com.dtstack.flinkx.connector.kingbase.table;

import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.table.JdbcDynamicTableFactory;
import com.dtstack.flinkx.connector.kingbase.dialect.KingbaseDialect;
import com.dtstack.flinkx.connector.kingbase.sink.KingbaseOutputFormat;
import com.dtstack.flinkx.connector.kingbase.source.KingbaseInputFormat;

import static com.dtstack.flinkx.connector.kingbase.util.KingbaseConstants.IDENTIFIER;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/05/13 20:10
 */
public class KingbaseDynamicTableFactory extends JdbcDynamicTableFactory {

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    protected JdbcDialect getDialect() {
        return new KingbaseDialect();
    }

    @Override
    protected JdbcInputFormatBuilder getInputFormatBuilder() {
        return new JdbcInputFormatBuilder(new KingbaseInputFormat());
    }

    @Override
    protected JdbcOutputFormatBuilder getOutputFormatBuilder() {
        return new JdbcOutputFormatBuilder(new KingbaseOutputFormat());
    }
}
