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

package com.dtstack.chunjun.connector.cassandra.sink;

import com.dtstack.chunjun.connector.cassandra.config.CassandraSinkConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;
import com.dtstack.chunjun.throwable.NoRestartException;

import org.apache.commons.lang3.StringUtils;

public class CassandraOutputFormatBuilder
        extends BaseRichOutputFormatBuilder<CassandraOutputFormat> {

    public CassandraOutputFormatBuilder() {
        super(new CassandraOutputFormat());
    }

    public void setSinkConfig(CassandraSinkConfig config) {
        super.setConfig(config);
        format.setSinkConfig(config);
    }

    public void setConverter(AbstractRowConverter rowConverter) {
        format.setRowConverter(rowConverter);
    }

    @Override
    protected void checkFormat() {
        CassandraSinkConfig sinkConf = format.getSinkConfig();

        StringBuilder stringBuilder = new StringBuilder(256);

        if (StringUtils.isBlank(sinkConf.getHost())) {
            stringBuilder.append("No host supplied;\n");
        }

        if (StringUtils.isBlank(sinkConf.getTableName())) {
            stringBuilder.append("No table-name supplied;\n");
        }

        if (stringBuilder.length() > 0) {
            throw new NoRestartException(stringBuilder.toString());
        }
    }
}
