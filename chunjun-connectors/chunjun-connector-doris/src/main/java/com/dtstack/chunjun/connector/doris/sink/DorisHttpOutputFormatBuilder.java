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

package com.dtstack.chunjun.connector.doris.sink;

import com.dtstack.chunjun.connector.doris.options.DorisConf;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import java.util.List;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2021/11/8
 */
public class DorisHttpOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private final DorisHttpOutputFormat format;

    public DorisHttpOutputFormatBuilder() {
        super.format = format = new DorisHttpOutputFormat();
    }

    public void setDorisOptions(DorisConf options) {
        JdbcConf jdbcConf = options.setToJdbcConf();
        format.setOptions(options);
        format.setConfig(jdbcConf);
    }

    public void setColumns(List<String> columns) {
        format.setColumns(columns);
    }

    @Override
    protected void checkFormat() {}
}
