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
package com.dtstack.flinkx.connector.inceptor.source;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.enums.Semantic;

import org.apache.commons.lang.StringUtils;

/**
 * @author dujie @Description
 * @createTime 2022-01-20 04:17:00
 */
public class InceptorInputFormatBuilder extends JdbcInputFormatBuilder {

    public InceptorInputFormatBuilder(JdbcInputFormat format) {
        super(format);
    }

    @Override
    protected void checkFormat() {
        JdbcConf conf = format.getJdbcConf();
        StringBuilder sb = new StringBuilder(256);

        if (StringUtils.isBlank(conf.getJdbcUrl())) {
            sb.append("No jdbc url supplied;\n");
        }

        if (conf.getParallelism() > 1) {
            if (StringUtils.isBlank(conf.getSplitPk())) {
                sb.append("Must specify the split column when the channel is greater than 1;\n");
            } else {
                FieldConf field =
                        FieldConf.getSameNameMetaColumn(conf.getColumn(), conf.getSplitPk());
                if (field == null) {
                    sb.append("split column must in columns;\n");
                } else if (!ColumnType.isNumberType(field.getType())) {
                    sb.append("split column's type must be number type;\n");
                }
            }
        }

        try {
            Semantic.getByName(conf.getSemantic());
        } catch (Exception e) {
            sb.append(String.format("unsupported semantic type %s", conf.getSemantic()));
        }

        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
