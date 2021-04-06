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

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.List;

/**
 * Reason:
 * Date: 2018/6/26
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public interface IParser {

    /**
     * 是否满足该解析类型
     *
     * @param sql
     * @return
     */
    boolean verify(String sql);

    /**
     * 执行sql
     * @param sql
     * @param tableEnvironment
     * @param statementSet
     * @param jarUrlList
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    void execSql(String sql, StreamTableEnvironment tableEnvironment, StatementSet statementSet, List<URL> jarUrlList) throws InvocationTargetException, IllegalAccessException;
}
