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

package com.dtstack.flinkx.phoenix5.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.phoenix5.Phoenix5DatabaseMeta;
import com.dtstack.flinkx.phoenix5.format.Phoenix5OutputFormat;
import com.dtstack.flinkx.rdb.datawriter.JdbcDataWriter;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.rdb.util.DbUtil;

import java.util.Collections;

/**
 * phoenix writer plugin
 *
 * Company: www.dtstack.com
 * @author wuhui
 */
public class Phoenix5Writer extends JdbcDataWriter {

    public Phoenix5Writer(DataTransferConfig config) {
        super(config);
        setDatabaseInterface(new Phoenix5DatabaseMeta());
        dbUrl = DbUtil.formatJdbcUrl(dbUrl, Collections.singletonMap("zeroDateTimeBehavior", "convertToNull"));
    }

    @Override
    protected JdbcOutputFormatBuilder getBuilder() {
        return new JdbcOutputFormatBuilder(new Phoenix5OutputFormat());
    }
}
