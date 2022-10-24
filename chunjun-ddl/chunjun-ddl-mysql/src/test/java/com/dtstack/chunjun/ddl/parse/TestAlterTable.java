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
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

public class TestAlterTable {

    @Test
    public void testAlterTableAttribute() throws SqlParseException {
        String sql =
                "ALTER TABLE t23 ENGINE = InnoDB,TABLESPACE t1 STORAGE DISK, MAX_ROWS = 12,MIN_ROWS = 12,CHARACTER SET utf8, COLLATE utf8_bin";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void alterMulty() throws SqlParseException {
        String sql =
                "alter table `abcq`"
                        + "    add index index1 (id) ,"
                        + "    add index index2 (id),"
                        + "    add CONSTRAINT `abc1` unique key `unique_name1322` (name(9))";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void testAlterAddColumn() throws SqlParseException {
        String sql = "ALTER TABLE t1 ADD COLUMN c1 int";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ADD c1 int";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ADD `column` int";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql =
                "ALTER TABLE t1 ADD COLUMN c1 int DEFAULT (RAND() * RAND()) NOT NULL  AUTO_INCREMENT VISIBLE  UNIQUE KEY PRIMARY KEY COMMENT 'comment'";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql =
                "ALTER TABLE t1 ADD COLUMN (c1 int DEFAULT (RAND() * RAND()) NOT NULL  AUTO_INCREMENT VISIBLE  UNIQUE KEY PRIMARY KEY COMMENT 'comment', c2 decimal(12,2) not null )";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql =
                "ALTER TABLE t1 ADD COLUMN c1 int DEFAULT (RAND() * RAND()) NOT NULL  AUTO_INCREMENT VISIBLE  UNIQUE KEY PRIMARY KEY COMMENT 'comment'  AFTER c0";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void testAlterAddIndex() throws SqlParseException {
        String sql = "ALTER TABLE t1 ADD INDEX USING BTREE (c1(12) DESC,c2 ASC,c3)";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ADD FULLTEXT (c1,c2)";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ADD FULLTEXT KEY (c1,c2)";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ADD FULLTEXT INDEX inde1 (c1,c2) COMMENT 'comment1' ";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql =
                "ALTER TABLE t1 ADD FULLTEXT INDEX inde1 (c1,c2) COMMENT 'comment1' ,ADD FULLTEXT INDEX inde2 (c1,c2) COMMENT 'comment1', add index index3(c3)";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void testAlterAddConstraint() throws SqlParseException {
        String sql = "ALTER TABLE t1 ADD a  PRIMARY KEY (c1)";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ADD  PRIMARY KEY (c1)";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ADD  PRIMARY KEY (c1) USING BTREE";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ADD  PRIMARY KEY USING BTREE (c1) ";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql =
                "ALTER TABLE t1 ADD unqiueIndex UNIQUE i1 USING HASH  (c1) "
                        + "KEY_BLOCK_SIZE 12 USING BTREE COMMENT 'comment'  WITH PARSER parse SECONDARY_ENGINE_ATTRIBUTE 'attribute1'";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql =
                "ALTER TABLE t1 ADD unqiueIndex  FOREIGN KEY (c1,c2) REFERENCES t2 (c1) MATCH FULL ON DELETE NO ACTION";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ADD CONSTRAINT unqiueIndex  CHECK (c1>10) NOT ENFORCED";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void testAlterDrop() throws SqlParseException {
        String sql = "ALTER TABLE t1 DROP  CHECK c1";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 DROP  CONSTRAINT c1";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 DROP  COLUMN c1";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 DROP  c1";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 DROP  INDEX c1";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 DROP KEY c1";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 DROP  PRIMARY KEY";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 DROP  FOREIGN KEY c1";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void testAlterAlter() throws SqlParseException {
        String sql = "ALTER TABLE t1 ALTER  CHECK c1 NOT ENFORCED";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALTER  CHECK c1 ENFORCED";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALTER  CONSTRAINT c1 NOT ENFORCED";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALTER  CONSTRAINT c1 ENFORCED";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALTER  INDEX c1 VISIBLE";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALTER  INDEX c1 INVISIBLE";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALTER c1 SET DEFAULT 12";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALTER c1 SET DEFAULT 'x12'";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALTER c1 SET DEFAULT (c2*2)";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALTER COLUMN c1 SET DEFAULT (c2*2)";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALTER c1 SET VISIBLE ";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALTER c1  SET INVISIBLE ";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALTER c1 DROP DEFAULT";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALTER COLUMN c1 DROP DEFAULT";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void testAlterAlgorithm() throws SqlParseException {
        String sql = "ALTER TABLE t1 ALGORITHM  DEFAULT";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALGORITHM  INSTANT";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALGORITHM  INPLACE";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ALGORITHM  = COPY";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void testAlterChangeColumn() throws SqlParseException {
        String sql = "ALTER TABLE t1 CHANGE  c1 c2 int not null ";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 CHANGE  c1 c2 int not null  after c3";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 CHANGE  c1 c2 int not null  FIRST c3";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void testAlterCharacter() throws SqlParseException {
        String sql = "ALTER TABLE t1 CONVERT TO CHARACTER SET utf8 COLLATE = utf8_bin";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1  DEFAULT CHARACTER SET = utf8 COLLATE = utf8_bin";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1  DEFAULT CHARACTER SET = utf8,  COLLATE = utf8_bin";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1   CONVERT TO  COLLATE = utf8_bin";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1   CONVERT TO  CHARACTER SET = utf8";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1  DEFAULT CHARACTER SET = utf8, COLLATE = utf8_bin";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1  CHARACTER SET = utf8 ,COLLATE utf8_bin";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 DEFAULT CHARACTER SET = utf8 ,COLLATE utf8_bin";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void testAlterKeys() throws SqlParseException {
        String sql = "ALTER TABLE t1 disable keys";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 ENABLE keys";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void testAlterTableSpace() throws SqlParseException {
        String sql = "ALTER TABLE t1 DISCARD TABLESPACE";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 IMPORT TABLESPACE";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void testAlterForce() throws SqlParseException {
        String sql = "ALTER TABLE t1 FORCE";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void testAlterLock() throws SqlParseException {
        String sql = "ALTER TABLE t1 LOCK DEFAULT";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 LOCK NONE";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 LOCK SHARED";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 LOCK EXCLUSIVE";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void testAlterModifyColumn() throws SqlParseException {
        String sql = "ALTER TABLE t1 modify  c2 int not null ";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 modify  c2 int not null  after c3";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 modify c2 int not null  FIRST c3";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void testAlterOrderBY() throws SqlParseException {
        String sql = "ALTER TABLE t1 order by c1";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 order by c1, c2";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 order by c2 desc";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 order by c1 desc, c2 asc";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void alterRename() throws SqlParseException {
        String sql = "ALTER TABLE t1 rename column c1 to c2";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 rename index c1 to c2";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 rename key c1 to c2";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 rename t2";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 rename to t2";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 rename as t2";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @Test
    public void alterValidation() throws SqlParseException {
        String sql = "ALTER TABLE t1 with VALIDATION";
        SqlNode sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));

        sql = "ALTER TABLE t1 without VALIDATION";
        sqlNode = test(sql);
        System.out.println(sqlNode.toSqlString(MysqlSqlDialect.DEFAULT));
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
