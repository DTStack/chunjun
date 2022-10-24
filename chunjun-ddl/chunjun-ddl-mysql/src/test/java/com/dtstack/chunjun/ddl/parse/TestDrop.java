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

public class TestDrop {

    @Test
    public void testDropView() throws SqlParseException {
        System.out.println(test("DROP view v1"));
        System.out.println(test("DROP view v1,v2,d1.v3 CASCADE"));
        System.out.println(test("DROP view if EXISTS v1,v2,d1.v3 CASCADE"));
    }

    @Test
    public void testDropTrigger() throws SqlParseException {
        System.out.println(test("DROP TRIGGER trigger1"));
        System.out.println(test("DROP TRIGGER db1.trigger1"));
        System.out.println(test("DROP TRIGGER if EXISTS db1.trigger1"));
    }

    @Test
    public void testDropTableSpace() throws SqlParseException {
        System.out.println(test("DROP TABLESPACE t1"));
        System.out.println(test("DROP UNDO TABLESPACE t1"));
        System.out.println(test("DROP UNDO TABLESPACE t1 ENGINE innodb"));
    }

    @Test
    public void testDropTable() throws SqlParseException {
        System.out.println(test("DROP TEMPORARY TABLE t1"));
        System.out.println(test("DROP TABLE t1,d1.t1"));
        System.out.println(test("DROP TABLE t1 RESTRICT"));
        System.out.println(test("DROP TEMPORARY TABLE IF EXISTS t1 RESTRICT"));
    }

    @Test
    public void testDropSystem() throws SqlParseException {
        System.out.println(test("DROP SPATIAL REFERENCE SYSTEM 4120"));
        System.out.println(test("DROP SPATIAL REFERENCE SYSTEM IF EXISTS 4120"));
    }

    @Test
    public void testDropServer() throws SqlParseException {
        System.out.println(test("DROP server server1"));
        System.out.println(test("DROP server IF EXISTS  server1"));
    }

    @Test
    public void testDropProcedure() throws SqlParseException {
        System.out.println(test("DROP PROCEDURE procedure1"));
        System.out.println(test("DROP PROCEDURE IF EXISTS  procedure1"));
    }

    @Test
    public void testDropFunction() throws SqlParseException {
        System.out.println(test("DROP FUNCTION f1"));
        System.out.println(test("DROP FUNCTION IF EXISTS  f1"));
    }

    @Test
    public void testDropLogfileGroup() throws SqlParseException {
        System.out.println(test("DROP LOGFILE GROUP file1 engine = innodb"));
    }

    @Test
    public void testDropIndex() throws SqlParseException {
        System.out.println(test("DROP INDEX `PRIMARY` ON t"));
        System.out.println(test("DROP INDEX index1 ON t"));
        System.out.println(test("DROP INDEX index1 ON t ALGORITHM = DEFAULT"));
        System.out.println(test("DROP INDEX index1 ON t ALGORITHM = INPLACE"));
        System.out.println(test("DROP INDEX index1 ON t LOCK  DEFAULT "));
        System.out.println(test("DROP INDEX index1 ON t LOCK = SHARED"));
    }

    @Test
    public void testDropEvent() throws SqlParseException {
        System.out.println(test("DROP EVENT e1"));
        System.out.println(test("DROP EVENT IF EXISTS e1"));
    }

    @Test
    public void testDropDataBase() throws SqlParseException {
        System.out.println(test("DROP DATABASE d1"));
        System.out.println(test("DROP DATABASE IF EXISTS d1"));

        System.out.println(test("DROP SCHEMA d1"));
        System.out.println(test("DROP SCHEMA IF EXISTS d1"));
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
