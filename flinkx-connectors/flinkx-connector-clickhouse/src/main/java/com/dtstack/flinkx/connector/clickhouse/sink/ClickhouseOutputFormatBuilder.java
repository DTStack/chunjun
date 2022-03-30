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

package com.dtstack.flinkx.connector.clickhouse.sink;

import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormatBuilder;

import org.apache.commons.lang.StringUtils;

public class ClickhouseOutputFormatBuilder extends JdbcOutputFormatBuilder {

    public ClickhouseOutputFormatBuilder(JdbcOutputFormat format) {
        super(format);
    }

    @Override
    protected void checkFormat() {
        JdbcConf jdbcConf = format.getJdbcConf();
        StringBuilder sb = new StringBuilder(256);
        // username and password is nullable
        //        if (StringUtils.isBlank(jdbcConf.getUsername())) {
        //            sb.append("No username supplied;\n");
        //        }
        //        if (StringUtils.isBlank(jdbcConf.getPassword())) {
        //            sb.append("No password supplied;\n");
        //        }
        if (StringUtils.isBlank(jdbcConf.getJdbcUrl())) {
            sb.append("No jdbc url supplied;\n");
        }
        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
