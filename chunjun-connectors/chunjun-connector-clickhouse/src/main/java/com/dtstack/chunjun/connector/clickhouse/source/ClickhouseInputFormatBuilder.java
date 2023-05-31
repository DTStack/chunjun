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

package com.dtstack.chunjun.connector.clickhouse.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.enums.Semantic;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

public class ClickhouseInputFormatBuilder extends JdbcInputFormatBuilder {
    public ClickhouseInputFormatBuilder(JdbcInputFormat format) {
        super(format);
    }

    @Override
    protected void checkFormat() {
        JdbcConfig config = format.getJdbcConfig();
        StringBuilder sb = new StringBuilder(256);

        if (StringUtils.isBlank(config.getJdbcUrl())) {
            sb.append("No jdbc url supplied;\n");
        }
        if (config.isIncrement()) {
            if (StringUtils.isBlank(config.getIncreColumn())) {
                sb.append("increColumn can't be empty when increment is true;\n");
            }
            config.setSplitPk(config.getIncreColumn());
            if (config.getParallelism() > 1) {
                config.setSplitStrategy("mod");
            }
        }

        if (config.getParallelism() > 1) {
            if (StringUtils.isBlank(config.getSplitPk())) {
                sb.append("Must specify the split column when the channel is greater than 1;\n");
            } else {
                FieldConfig field =
                        FieldConfig.getSameNameMetaColumn(config.getColumn(), config.getSplitPk());
                if (field == null) {
                    sb.append("split column must in columns;\n");
                } else if (!ColumnType.isNumberType(field.getType().getType())) {
                    sb.append("split column's type must be number type;\n");
                }
            }
        }

        if (StringUtils.isNotBlank(config.getStartLocation())) {
            String[] startLocations = config.getStartLocation().split(ConstantValue.COMMA_SYMBOL);
            if (startLocations.length != 1 && startLocations.length != config.getParallelism()) {
                sb.append("startLocations is ")
                        .append(Arrays.toString(startLocations))
                        .append(", length = [")
                        .append(startLocations.length)
                        .append("], but the channel is [")
                        .append(config.getParallelism())
                        .append("];\n");
            }
        }
        try {
            Semantic.getByName(config.getSemantic());
        } catch (Exception e) {
            sb.append(String.format("unsupported semantic type %s", config.getSemantic()));
        }

        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
