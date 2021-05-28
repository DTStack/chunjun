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

package com.dtstack.flinkx.postgresql.writer;

import com.dtstack.flinkx.postgresql.format.PostgresqlOutputFormat;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormatBuilder;

import java.util.Arrays;

public class PostgresqlOutputFormatBuilder extends JdbcOutputFormatBuilder {
    private PostgresqlOutputFormat format;

    public PostgresqlOutputFormatBuilder(PostgresqlOutputFormat format) {
        super(format);
        this.format = format;
    }

    public void setSourceType(String sourceType) {
        this.format.setSourceType(sourceType);
    }

    @Override
    protected void checkFormat() {
        super.checkFormat();

        //校验sourceType类型
        if (Arrays.stream(PostgresqlOutputFormat.SourceType.values()).noneMatch(i -> i.name().equalsIgnoreCase(format.getSourceType()))) {
            throw new IllegalArgumentException("sourceType must be one of [POSTGRESQL, ADB], current sourceType is [" + format.getSourceType() + ("] \n"));
        }
    }
}
