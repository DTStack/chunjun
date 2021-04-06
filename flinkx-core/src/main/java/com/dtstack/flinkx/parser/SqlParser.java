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

import com.dtstack.flinkx.exec.ParamsInfo;
import com.dtstack.flinkx.util.DtStringUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reason:
 * Date: 2018/6/22
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class SqlParser {
    private static final Logger LOG = LoggerFactory.getLogger(SqlParser.class);

    private static final char SQL_DELIMITER = ';';
    private static final Pattern ADD_FILE_AND_JAR_PATTERN = Pattern.compile("(?i).*add\\s+file\\s+.+|(?i).*add\\s+jar\\s+.+");
    private static final List<IParser> sqlParserList =
            Lists.newArrayList(
                    CreateFuncParser.newInstance(),
                    CreateTableParser.newInstance(),
                    InsertSqlParser.newInstance(),
                    CreateTmpTableParser.newInstance());

    /**
     * flink support sql syntax
     * CREATE TABLE sls_stream() with ();
     * CREATE (TABLE|SCALA) FUNCTION fcnName WITH com.dtstack.com;
     * insert into tb1 select * from tb2;
     *
     * @param
     */
    public static StatementSet parseSql(ParamsInfo paramsInfo, StreamTableEnvironment tableEnvironment) throws Exception {

        String sql = paramsInfo.getSql();
        if (StringUtils.isBlank(sql)) {
            throw new IllegalArgumentException("SQL must be not empty!");
        }

        sql = DtStringUtil
                .dealSqlComment(sql)
                .replaceAll("\r\n", " ")
                .replaceAll("\n", " ")
                .replace("\t", " ").trim();

        StatementSet statement = tableEnvironment.createStatementSet();

        List<String> sqlArr = DtStringUtil.splitIgnoreQuota(sql, SQL_DELIMITER);
        sqlArr = removeAddFileAndJarStmt(sqlArr);
        for (String childSql : sqlArr) {
            if (Strings.isNullOrEmpty(childSql)) {
                continue;
            }
            for (IParser sqlParser : sqlParserList) {
                if (!sqlParser.verify(childSql)) {
                    continue;
                }
                sqlParser.execSql(childSql, tableEnvironment, statement, paramsInfo.getJarUrlList());
            }
        }

        return statement;
    }

    /**
     * remove add file and jar with statment etc. add file /etc/krb5.conf, add jar xxx.jar;
     */
    private static List<String> removeAddFileAndJarStmt(List<String> stmts) {
        List<String> cleanedStmts = Lists.newArrayList();
        for (String stmt : stmts) {
            Matcher matcher = ADD_FILE_AND_JAR_PATTERN.matcher(stmt);
            if (!matcher.matches()) {
                cleanedStmts.add(stmt);
            }
        }
        return cleanedStmts;
    }
}
