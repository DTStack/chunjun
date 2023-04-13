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

package com.dtstack.chunjun.sql.parser;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.commons.lang3.StringUtils;

import java.net.URL;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lihongwei
 * @create 2023-02-27
 */
public class SetStmtParser extends AbstractStmtParser {

    private static final String SET_PATTERN_STR = "(?i)\\s*SET\\s+(\\S+)\\s?=\\s?(\\S+)\\s?";

    private static final Pattern SET_PATTERN = Pattern.compile(SET_PATTERN_STR);

    @Override
    public boolean canHandle(String stmt) {
        return StringUtils.isNotBlank(stmt)
                && stmt.trim().toLowerCase().startsWith("set")
                && SET_PATTERN.matcher(stmt).find();
    }

    @Override
    public void execStmt(
            String stmt,
            StreamTableEnvironment tEnv,
            StatementSet statementSet,
            List<URL> jarUrlList) {
        if (SET_PATTERN.matcher(stmt).find()) {
            Matcher matcher = SET_PATTERN.matcher(stmt);
            if (matcher.find()) {
                String key = matcher.group(1);
                String value = matcher.group(2);

                Configuration configuration = tEnv.getConfig().getConfiguration();
                configuration.setString(key, value);
            }
        }
    }

    public Configuration execStmt(String stmt) {
        Configuration configuration = new Configuration();
        if (SET_PATTERN.matcher(stmt).find()) {
            Matcher matcher = SET_PATTERN.matcher(stmt);
            if (matcher.find()) {
                String key = matcher.group(1);
                String value = matcher.group(2);
                configuration.setString(key, value);
            }
        }
        return configuration;
    }
}
