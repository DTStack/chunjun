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
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.Test;

public class TestCreate {
    @org.junit.Test
    public void testCreateTable() throws SqlParseException {
        // Sql语句
        String sql = "create table c1  like t3 ";
        System.out.println(test(sql));
        sql =
                "CREATE TABLE `t2` (\n"
                        + "  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n"
                        + "  `tiny_un_z_c32` tinyint(3) unsigned zerofill NOT NULL,\n"
                        + "  `small_c` smallint(6) NOT NULL,\n"
                        + "  `small_un_c` smallint(5) unsigned DEFAULT NULL,\n"
                        + "  `small_un_z_c` smallint(5) unsigned zerofill DEFAULT NULL,\n"
                        + "  `medium_c` mediumint(9) NOT NULL,\n"
                        + "  `medium_un_c` mediumint(8) unsigned NOT NULL COMMENT '请问',\n"
                        + "  `medium_un_z_c` mediumint(8) unsigned zerofill DEFAULT NULL,\n"
                        + "  `int_c` int(11) NOT NULL,\n"
                        + "  `int_un_c` int(10) unsigned DEFAULT NULL,\n"
                        + "  `int_un_z_c2` int(10) unsigned zerofill DEFAULT NULL,\n"
                        + "  `int11_c123` int(11) NOT NULL DEFAULT '123' COMMENT '213哈哈',\n"
                        + "  `big_c` bigint(20) DEFAULT NULL,\n"
                        + "  `big_un_c` bigint(20) unsigned DEFAULT NULL,\n"
                        + "  `big_un_z_c` bigint(20) unsigned zerofill DEFAULT NULL,\n"
                        + "  `varchar_c` varchar(255) DEFAULT NULL,\n"
                        + "  `char_c` char(3) DEFAULT NULL,\n"
                        + "  `real_c` double DEFAULT NULL,\n"
                        + "  `float_c` float DEFAULT NULL,\n"
                        + "  `float_un_c` float unsigned DEFAULT NULL,\n"
                        + "  `float_un_z_c` float unsigned zerofill DEFAULT NULL,\n"
                        + "  `double_c` double DEFAULT NULL,\n"
                        + "  `double_un_c` double unsigned DEFAULT NULL,\n"
                        + "  `double_un_z_c` double unsigned zerofill DEFAULT NULL,\n"
                        + "  `decimal_c` decimal(8,4) DEFAULT NULL,\n"
                        + "  `decimal_un_c` decimal(8,4) unsigned DEFAULT NULL,\n"
                        + "  `decimal_un_z_c` decimal(8,4) unsigned zerofill DEFAULT NULL,\n"
                        + "  `numeric_c` decimal(6,0) DEFAULT NULL,\n"
                        + "  `big_decimal_c` decimal(65,1) DEFAULT NULL,\n"
                        + "  `bit1_c` bit(1) DEFAULT NULL,\n"
                        + "  `tiny1_c` tinyint(1) DEFAULT NULL,\n"
                        + "  `boolean_c` tinyint(1) DEFAULT NULL,\n"
                        + "  `date_c` date DEFAULT NULL,\n"
                        + "  `time_c` time DEFAULT NULL,\n"
                        + "  `datetime3_c` datetime(3) DEFAULT NULL,\n"
                        + "  `datetime6_c` datetime(6) DEFAULT NULL,\n"
                        + "  `timestamp_c` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
                        + "  `file_uuid` binary(16) DEFAULT NULL,\n"
                        + "  `bit_c` bit(64) DEFAULT NULL,\n"
                        + "  `text_c` text,\n"
                        + "  `tiny_text_c` tinytext,\n"
                        + "  `medium_text_c` mediumtext,\n"
                        + "  `long_text_c` longtext,\n"
                        + "  `tiny_blob_c` tinyblob,\n"
                        + "  `blob_c` blob,\n"
                        + "  `medium_blob_c` mediumblob,\n"
                        + "  `long_blob_c` longblob,\n"
                        + "  `year_c` year(4) DEFAULT NULL,\n"
                        + "  `enum_c` enum('红色','white') DEFAULT '红色',\n"
                        + "  `json_c` json DEFAULT NULL,\n"
                        + "  `column_51` varchar(22) NOT NULL DEFAULT '321111112哈哈' COMMENT '哈哈哈 213 1121方式1221\\n121',\n"
                        + "  `column_52` int(11) NOT NULL,\n"
                        + "  PRIMARY KEY (`id`),\n"
                        + "  UNIQUE KEY `t1_column_52_uindex` (`column_52`),\n"
                        + "  UNIQUE KEY `t1_small_c_uindex` (`small_c`),\n"
                        + "  UNIQUE KEY `t1_tiny_un_z_c32_uindex` (`tiny_un_z_c32`),\n"
                        + "  UNIQUE KEY `t1_medium_c_uindex` (`medium_c`),\n"
                        + "  UNIQUE KEY `t1_int11_c123_uindex` (`int11_c123`),\n"
                        + "  UNIQUE KEY `t1_medium_un_c_uindex` (`medium_un_c`),\n"
                        + "  UNIQUE KEY `t1_column_51_uindex` (`column_51`),\n"
                        + "  UNIQUE KEY `t1_int_un_c_uindex` (`int_un_c`)\n"
                        + ") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COMMENT='1113'";
        SqlNode test = test(sql);
        System.out.println(test.toSqlString(MysqlSqlDialect.DEFAULT));
    }

    @org.junit.Test
    public void testCreateTableWithColumn() throws SqlParseException {
        // Sql语句
        String sql =
                "create table t1 (Ca1 int DEFAULT (RAND() * RAND()) NOT NULL  AUTO_INCREMENT VISIBLE  UNIQUE KEY PRIMARY KEY COMMENT 'comment哈哈' )";
        System.out.println(test(sql).toSqlString(MysqlSqlDialect.DEFAULT));

        sql =
                "CREATE TABLE `qianyi` (\n"
                        + "  `id` int DEFAULT NULL,\n"
                        + "  `name` varchar(100) COLLATE utf8mb4_bin DEFAULT NULL\n"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";
        System.out.println(test(sql).toSqlString(MysqlSqlDialect.DEFAULT));

        sql =
                "CREATE TABLE t23 (\n"
                        + "  `name` varchar(255) DEFAULT '12' NOT NULL,\n"
                        + "  PRIMARY KEY (`id`)\n"
                        + ")";
        System.out.println(test(sql));

        sql = "create table table_name2\n" + "(\n" + "\tnone int\n" + ")\n";
        System.out.println(test(sql));

        sql = "create table table_name2\n" + "(\n" + "\tdisk int\n" + ")\n";
        System.out.println(test(sql));

        sql =
                "create table d1.t1 (c1 int NOT NULL DEFAULT (RAND() * RAND()) VISIBLE AUTO_INCREMENT UNIQUE PRIMARY KEY COMMENT 'comment' )";
        System.out.println(test(sql));
        sql =
                "create table t1 (c1 int NOT NULL DEFAULT (UUID_TO_BIN(UUID())) VISIBLE AUTO_INCREMENT UNIQUE PRIMARY KEY COMMENT 'comment' )";
        System.out.println(test(sql));
        sql =
                "create table t1 (c1 int NOT NULL DEFAULT 'abc' VISIBLE AUTO_INCREMENT UNIQUE PRIMARY KEY COMMENT 'comment' )";
        System.out.println(test(sql));
        sql =
                "create table t1 (c1 int NOT NULL DEFAULT 1 VISIBLE AUTO_INCREMENT UNIQUE PRIMARY KEY COMMENT 'comment' )";
        System.out.println(test(sql));

        sql =
                " create table t1 (c1 int NOT NULL DEFAULT 1 VISIBLE AUTO_INCREMENT UNIQUE PRIMARY KEY COMMENT 'comment' references t3 (column_1) on delete set null )";
        System.out.println(test(sql));
    }

    @org.junit.Test
    public void testCreateTableWithAsColumn() throws SqlParseException {

        String sql =
                "CREATE TABLE triangle (\n"
                        + "  sidea DOUBLE,\n"
                        + "  sideb DOUBLE,\n"
                        + "  sidec DOUBLE GENERATED ALWAYS AS (SQRT(sidea * sidea + sideb * sideb)) UNIQUE COMMENT 'comment'"
                        + ")";
        System.out.println(test(sql));
        sql =
                "CREATE TABLE triangle (\n"
                        + "  sidea DOUBLE,\n"
                        + "  sideb DOUBLE,\n"
                        + "  sidec DOUBLE GENERATED ALWAYS AS (SQRT(sidea * sidea + sideb * sideb))  KEY COMMENT 'comment'"
                        + ")";
        System.out.println(test(sql));
        sql =
                "CREATE TABLE triangle (\n"
                        + "  sidea DOUBLE,\n"
                        + "  sideb DOUBLE,\n"
                        + "  sidec DOUBLE GENERATED ALWAYS AS (SQRT(sidea * sidea + sideb * sideb)) NOT NULL COMMENT 'comment'"
                        + ")";
        System.out.println(test(sql));
    }

    @org.junit.Test
    public void testCreateTableWithColumnAndIndex() throws SqlParseException {
        // Sql语句
        String sql =
                "create table t1 ("
                        + "INDEX index1 USING BTREE (c1 DESC,c2 ASC,c3) "
                        + ",INDEX index1 USING BTREE (c1) "
                        + ",c1 int NOT NULL DEFAULT 1 VISIBLE AUTO_INCREMENT UNIQUE PRIMARY KEY COMMENT 'comment'  "
                        + ",INDEX  USING BTREE (c1 DESC,c2 ASC,c3) USING BTREE COMMENT 'string' "
                        + ",INDEX (c1 DESC,c2 ASC,c3) COMMENT 'string' VISIBLE"
                        + ",FULLTEXT INDEX index2  (c4 DESC,c5 ASC,c6)"
                        + ",FULLTEXT  index3  (c4 DESC,c5 ASC,c6)"
                        + ",FULLTEXT   (c4 DESC,c5 ASC,c6)"
                        + " )";
        System.out.println(test(sql));
    }

    @org.junit.Test
    public void testCreateTableWithColumnAndIndexAndConstraint() throws SqlParseException {
        // Sql语句
        String sql =
                "create table t1 ("
                        + "INDEX index1 USING BTREE (c1 DESC,c2 ASC,c3) "
                        + ",c1 int DEFAULT 1 NOT NULL  VISIBLE AUTO_INCREMENT UNIQUE PRIMARY KEY COMMENT 'comment' STORAGE DISK  COLUMN_FORMAT DYNAMIC constraint for1  check (c1>10) ENFORCED  "
                        + ",INDEX  USING BTREE (c1 DESC,c2 ASC,c3) "
                        + ",INDEX (c1 DESC,c2 ASC,c3) "
                        + ",FULLTEXT INDEX index2  (c4 DESC,c5 ASC,c6)"
                        + ",FULLTEXT  index3  (c4 DESC,c5 ASC,c6)"
                        + ",FULLTEXT  (c4 DESC,c5 ASC,c6) WITH PARSER `ngram` "
                        + ",CONSTRAINT  t3_pk  PRIMARY KEY (c4) KEY_BLOCK_SIZE = 12 COMMENT 'string'"
                        + ",CONSTRAINT t3_pk1  UNIQUE KEY  BTREE (c1)"
                        + ",constraint for1 foreign key (c1) references t3 (column_1) on delete set null"
                        + ",constraint for1 foreign key (c1) references t3 (column_1) on update set null"
                        + ",constraint for1 foreign key (c1) references t3 (column_1)"
                        + ",constraint for1 check (c1>10) ENFORCED"
                        + " )";
        SqlNode test = test(sql);
        System.out.println(test.toSqlString(MysqlSqlDialect.DEFAULT));
        sql = "CREATE TABLE t1 (c1 INT ENGINE_ATTRIBUTE='{\"key\":\"value\"}') ";
        System.out.println(test(sql));
    }

    @org.junit.Test
    public void testCreateTableWithColumnAndIndexAndConstraintWithProperty()
            throws SqlParseException {
        // Sql语句
        String sql =
                "CREATE TABLE t5 ("
                        + "  column_1 int NOT NULL DEFAULT (12),"
                        + "  UNIQUE KEY t3_pk (column_1)\n"
                        + ")  DEFAULT CHARSET SET =utf8mb4 COLLATE=utf8mb4_0900_ai_ci  TABLESPACE t1 STORAGE DISK MAX_ROWS = 12.12 MIN_ROWS = 12 ";
        System.out.println(test(sql));
    }

    @Test
    public void testCreateDataBase() throws SqlParseException {
        String sql = "CREATE database d1 ";
        System.out.println(test(sql));

        sql = "CREATE schema d1 ";
        System.out.println(test(sql));

        sql = "CREATE schema if not exists d1 ENCRYPTION = 'Y' CHARACTER SET utf8";
        System.out.println(test(sql));
    }

    @Test
    public void testCreateIndex() throws SqlParseException {
        String sql = "CREATE UNIQUE INDEX idx1 ON t1 ((col1 + col2)) ALGORITHM DEFAULT LOCK = NONE";
        System.out.println(test(sql));
    }

    private SqlNode test(String sql) throws SqlParseException {
        // 解析配置 - mysql设置
        SqlParser.Config mysqlConfig =
                SqlParser.configBuilder()
                        // 定义解析工厂
                        .setParserFactory(ChunjunMySqlParserImpl.FACTORY)
                        .setConformance(SqlConformanceEnum.MYSQL_5)
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
