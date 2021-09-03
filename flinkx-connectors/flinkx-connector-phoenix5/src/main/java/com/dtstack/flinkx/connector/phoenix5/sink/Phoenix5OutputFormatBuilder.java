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
package com.dtstack.flinkx.connector.phoenix5.sink;

import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormatBuilder;

import org.apache.commons.lang.StringUtils;

/**
 * @author wujuan
 * @version 1.0
 * @date 2021/7/9 16:01 星期五
 * @email wujuan@dtstack.com
 * @company www.dtstack.com
 */
public class Phoenix5OutputFormatBuilder extends JdbcOutputFormatBuilder {

    public Phoenix5OutputFormatBuilder(JdbcOutputFormat format) {
        super(format);
    }

    @Override
    protected void checkFormat() {
        JdbcConf jdbcConf = format.getJdbcConf();
        StringBuilder sb = new StringBuilder(256);
        if (StringUtils.isBlank(jdbcConf.getJdbcUrl())) {
            sb.append("No jdbc url supplied;\n");
        }
        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
