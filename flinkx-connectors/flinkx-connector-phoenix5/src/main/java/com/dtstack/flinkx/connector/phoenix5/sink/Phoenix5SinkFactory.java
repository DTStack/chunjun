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

package com.dtstack.flinkx.connector.phoenix5.sink;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcSinkFactory;
import com.dtstack.flinkx.connector.phoenix5.Phoenix5Dialect;
import com.dtstack.flinkx.connector.phoenix5.converter.Phoenix5RawTypeConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;

/**
 * @author wujuan
 * @version 1.0
 * @date 2021/7/9 16:01 星期五
 * @email wujuan@dtstack.com
 * @company www.dtstack.com
 */
public class Phoenix5SinkFactory extends JdbcSinkFactory {

    public Phoenix5SinkFactory(SyncConf syncConf) {
        super(syncConf, new Phoenix5Dialect());
    }

    @Override
    protected JdbcOutputFormatBuilder getBuilder() {
        return new Phoenix5OutputFormatBuilder(new Phoenix5OutputFormat());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return Phoenix5RawTypeConverter::apply;
    }
}
