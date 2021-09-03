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

package com.dtstack.flinkx.sql.parser;

import com.dtstack.flinkx.throwable.DtSqlParserException;
import com.dtstack.flinkx.util.DtStringUtil;
import com.dtstack.flinkx.util.Splitter;

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import org.apache.commons.lang3.StringUtils;

import java.net.URL;
import java.util.List;

/**
 * Reason: Date: 2018/6/22 Company: www.dtstack.com
 *
 * @author xuchao
 */
public class SqlParser {

    private static final char SQL_DELIMITER = ';';

    /**
     * flink support sql syntax CREATE TABLE sls_stream() with (); CREATE (TABLE|SCALA) FUNCTION
     * fcnName WITH com.dtstack.com; insert into tb1 select * from tb2;
     *
     * @param
     */
    public static StatementSet parseSql(
            String sql, List<URL> urlList, StreamTableEnvironment tableEnvironment) {

        if (StringUtils.isBlank(sql)) {
            throw new IllegalArgumentException("SQL must be not empty!");
        }

        sql = DtStringUtil.dealSqlComment(sql);
        StatementSet statement = tableEnvironment.createStatementSet();
        Splitter splitter = new Splitter(SQL_DELIMITER);
        List<String> stmts = splitter.splitEscaped(sql);
        AbstractStmtParser stmtParser = createParserChain();

        stmts.stream()
                .filter(stmt -> !Strings.isNullOrEmpty(stmt.trim()))
                .forEach(
                        stmt -> {
                            try {
                                stmtParser.handleStmt(stmt, tableEnvironment, statement, urlList);
                            } catch (Exception e) {
                                // TODO 和 DtSqlParserException 要替换成 FlinkxSqlParseException 合并
                                throw new DtSqlParserException(stmt, e.getMessage(), e);
                            }
                        });

        return statement;
    }

    private static AbstractStmtParser createParserChain() {

        AbstractStmtParser uploadFileStmtParser = new UploadFileStmtParser();
        AbstractStmtParser createFunctionStmtParser = new CreateFunctionStmtParser();
        AbstractStmtParser insertStmtParser = new InsertStmtParser();

        uploadFileStmtParser.setNextStmtParser(createFunctionStmtParser);
        createFunctionStmtParser.setNextStmtParser(insertStmtParser);

        return uploadFileStmtParser;
    }
}
