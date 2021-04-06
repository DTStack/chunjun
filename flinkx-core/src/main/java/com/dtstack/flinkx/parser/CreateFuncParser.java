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


package com.dtstack.flinkx.parser;

import com.dtstack.flinkx.classloader.ClassLoaderManager;
import com.dtstack.flinkx.function.FunctionManager;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * parser register udf sql
 * Date: 2018/6/26
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class CreateFuncParser implements IParser {

    private static final String FUNC_PATTERN_STR = "(?i)\\s*create\\s+(scala|table|aggregate)\\s+function\\s+(\\S+)\\s+WITH\\s+(\\S+)";

    private static final Pattern FUNC_PATTERN = Pattern.compile(FUNC_PATTERN_STR);

    public static CreateFuncParser newInstance() {
        return new CreateFuncParser();
    }

    @Override
    public boolean verify(String sql) {
        return FUNC_PATTERN.matcher(sql).find();
    }

    @Override
    public void execSql(String sql, StreamTableEnvironment tableEnvironment, StatementSet statementSet, List<URL> jarUrlList) throws InvocationTargetException, IllegalAccessException {
        if (FUNC_PATTERN.matcher(sql).find()) {
            Matcher matcher = FUNC_PATTERN.matcher(sql);
            if (matcher.find()) {
                String type = matcher.group(1);
                String funcName = matcher.group(2);
                String className = matcher.group(3);

                ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
                URLClassLoader classLoader = ClassLoaderManager.loadExtraJar(jarUrlList, (URLClassLoader) currentClassLoader);
                FunctionManager.registerUDF(type, className, funcName, tableEnvironment, classLoader);
            }
        }
    }
}
