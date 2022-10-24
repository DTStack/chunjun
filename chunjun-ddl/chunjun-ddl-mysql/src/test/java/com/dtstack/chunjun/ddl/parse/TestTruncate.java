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

package com.dtstack.chunjun.ddl.parse;

import com.dtstack.chunjun.ddl.convent.mysql.parse.impl.ChunjunMySqlParserImpl;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

public class TestTruncate {

    @Test
    public void testTruncate() throws SqlParseException {
        System.out.println(test("TRUNCATE TABLE t1"));
        System.out.println(test("TRUNCATE TABLE d1.t1"));
    }

    private SqlNode test(String sql) throws SqlParseException {
        // 解析配置 - mysql设置
        SqlParser.Config mysqlConfig =
                SqlParser.configBuilder()
                        // 定义解析工厂
                        .setParserFactory(ChunjunMySqlParserImpl.FACTORY)
                        .setLex(Lex.MYSQL)
                        .build();
        // 创建解析器
        SqlParser parser = SqlParser.create(sql, mysqlConfig);
        // 解析sql
        return parser.parseStmt();
        // 还原某个方言的SQL
        //        System.out.println(sqlNode.toSqlString(OracleSqlDialect.DEFAULT));
    }
}
