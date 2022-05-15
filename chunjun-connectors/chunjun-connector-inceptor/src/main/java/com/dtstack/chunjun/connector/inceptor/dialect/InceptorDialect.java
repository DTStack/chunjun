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
package com.dtstack.chunjun.connector.inceptor.dialect;

import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;

import java.util.Arrays;
import java.util.Optional;

import static com.dtstack.chunjun.connector.inceptor.util.InceptorDbUtil.INCEPTOR_TRANSACTION_TYPE;

public abstract class InceptorDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "INCEPTOR";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:hive2:");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.apache.hive.jdbc.HiveDriver");
    }

    public String appendJdbcTransactionType(String jdbcUrl) {
        String transactionType = INCEPTOR_TRANSACTION_TYPE.substring(4);
        String[] split = jdbcUrl.split("\\?");
        StringBuilder stringBuilder = new StringBuilder(jdbcUrl);
        boolean flag = false;
        if (split.length == 2) {
            flag =
                    Arrays.stream(split[1].split("&"))
                            .anyMatch(s -> s.equalsIgnoreCase(transactionType));
            stringBuilder.append('&');
        } else {
            stringBuilder.append("?");
        }
        if (!flag) {
            return stringBuilder.append(transactionType).toString();
        }
        return jdbcUrl;
    }

    public abstract JdbcInputFormatBuilder getInputFormatBuilder();

    public abstract JdbcOutputFormatBuilder getOutputFormatBuilder();
}
