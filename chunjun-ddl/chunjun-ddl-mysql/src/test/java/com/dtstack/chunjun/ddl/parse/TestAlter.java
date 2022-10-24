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

public class TestAlter {
    @Test
    public void testAlterDataBase() throws SqlParseException {
        String sql =
                "ALTER database d1 CHARACTER SET = utf8 COLLATE = utf8_bin DEFAULT ENCRYPTION = 'Y' READ ONLY 0";
        SqlNode test = test(sql);
        System.out.println(test);

        sql = "ALTER database d1 ENCRYPTION = 'Y' READ ONLY = DEFAULT";
        test = test(sql);
        System.out.println(test);

        sql = "ALTER database d1 DEFAULT ENCRYPTION = 'N' READ ONLY 1";
        test = test(sql);
        System.out.println(test);
    }

    @Test
    public void testAlterEvent() throws SqlParseException {

        // todo not support
        // ALTER EVENT myevent
        //    ON SCHEDULE
        //      AT CURRENT_TIMESTAMP + INTERVAL 1 DAY
        //    DO
        //      TRUNCATE TABLE myschema.mytable;
        //
        String sql = "ALTER EVENT no_such_event ON SCHEDULE INTERVAL 1 DAY RENAME TO yourevent";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode);

        sql = "ALTER EVENT myevent DISABLE";
        sqlNode = test(sql);
        System.out.println(sqlNode);
    }

    @Test
    public void testFunction() throws SqlParseException {

        String sql =
                "ALTER FUNCTION  f1 COMMENT 'this is function'  SQL SECURITY INVOKER LANGUAGE SQL READS SQL DATA ";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode);

        sql =
                "ALTER FUNCTION  f1 COMMENT 'this is function'  LANGUAGE SQL MODIFIES SQL DATA SQL SECURITY DEFINER";
        sqlNode = test(sql);
        System.out.println(sqlNode);
    }

    @Test
    public void testInstance() throws SqlParseException {

        String sql = "ALTER INSTANCE DISABLE INNODB REDO_LOG ROTATE BINLOG MASTER KEY RELOAD TLS";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode);

        sql =
                "ALTER INSTANCE DISABLE INNODB REDO_LOG ROTATE BINLOG MASTER KEY RELOAD TLS FOR CHANNEL mysql_admin RELOAD KEYRING";
        sqlNode = test(sql);
        System.out.println(sqlNode);

        sql =
                "ALTER INSTANCE DISABLE INNODB REDO_LOG ROTATE BINLOG MASTER KEY RELOAD TLS NO ROLLBACK ON ERROR RELOAD KEYRING";
        sqlNode = test(sql);
        System.out.println(sqlNode);
    }

    @Test
    public void testProcedure() throws SqlParseException {

        String sql = "ALTER PROCEDURE pro_name COMMENT 'test1' SQL SECURITY DEFINER";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode);

        sql = "ALTER PROCEDURE pro_name COMMENT 'test1' SQL SECURITY DEFINER READS SQL DATA ";
        sqlNode = test(sql);
        System.out.println(sqlNode);
    }

    @Test
    public void testServer() throws SqlParseException {

        String sql = "ALTER SERVER s OPTIONS (USER 'sally')";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode);

        sql =
                "ALTER SERVER s OPTIONS (USER 'sally',HOST '198.51.100.106', DATABASE 'test', port 123)";
        sqlNode = test(sql);
        System.out.println(sqlNode);
    }

    @Test
    public void testLogfileGroup() throws SqlParseException {

        String sql =
                "ALTER LOGFILE GROUP lg_3"
                        + "    ADD UNDOFILE 'undo_10.dat'"
                        + "    INITIAL_SIZE=32M"
                        + "    ENGINE=NDBCLUSTER";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode);
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
