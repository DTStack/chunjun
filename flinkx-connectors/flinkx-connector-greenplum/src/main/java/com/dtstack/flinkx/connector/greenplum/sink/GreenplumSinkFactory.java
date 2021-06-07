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

package com.dtstack.flinkx.connector.greenplum.sink;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.greenplum.GreenplumDialect;
import com.dtstack.flinkx.connector.greenplum.converter.GreenplumRawTypeConverter;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcSinkFactory;
import com.dtstack.flinkx.converter.RawTypeConverter;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class GreenplumSinkFactory extends JdbcSinkFactory {

    public GreenplumSinkFactory(SyncConf syncConf) {
        super(syncConf);
        super.jdbcDialect = new GreenplumDialect();
    }

    @Override
    protected JdbcOutputFormatBuilder getBuilder() {
        return new JdbcOutputFormatBuilder(new GreenplumOutputFormat());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return GreenplumRawTypeConverter::apply;
    }
}
