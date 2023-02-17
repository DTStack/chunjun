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
package com.dtstack.chunjun.ddl.convent;

import com.dtstack.chunjun.cdc.EventType;
import com.dtstack.chunjun.cdc.ddl.DdlConvent;
import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;
import com.dtstack.chunjun.cdc.ddl.definition.TableOperator;
import com.dtstack.chunjun.ddl.convent.mysql.MysqlDdlConventImpl;
import com.dtstack.chunjun.ddl.convent.mysql.parse.impl.ChunjunMySqlParserImpl;
import com.dtstack.chunjun.mapping.MappingConfig;
import com.dtstack.chunjun.throwable.ConventException;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;

public class TestDdlConvent {
    private final DdlConvent convent = new MysqlDdlConventImpl();

    private final String DEFAULT_DATABASE = "dtstack";

    private final SqlParser.Config mysqlConfig =
            SqlParser.config()
                    // 定义解析工厂
                    .withParserFactory(ChunjunMySqlParserImpl.FACTORY)
                    .withConformance(SqlConformanceEnum.MYSQL_5)
                    .withLex(Lex.MYSQL);

    @Test
    public void testCreateTable() throws ConventException {
        String sql =
                "CREATE TABLE t1\n"
                        + "(\n"
                        + "    id                   SERIAL,\n"
                        + "    tiny_c               TINYINT,\n"
                        + "    tiny_un_c            TINYINT UNSIGNED,\n"
                        + "    tiny_un_z_c          TINYINT UNSIGNED ZEROFILL,\n"
                        + "    small_c              SMALLINT,\n"
                        + "    small_un_c           SMALLINT UNSIGNED,\n"
                        + "    small_un_z_c         SMALLINT UNSIGNED ZEROFILL,\n"
                        + "    medium_c             MEDIUMINT,\n"
                        + "    medium_un_c          MEDIUMINT UNSIGNED,\n"
                        + "    medium_un_z_c        MEDIUMINT UNSIGNED ZEROFILL,\n"
                        + "    int_c                INTEGER,\n"
                        + "    int_un_c             INTEGER UNSIGNED,\n"
                        + "    int_un_z_c           INTEGER UNSIGNED ZEROFILL,\n"
                        + "    int11_c              INT(11),\n"
                        + "    big_c                BIGINT,\n"
                        + "    big_un_c             BIGINT UNSIGNED,\n"
                        + "    big_un_z_c           BIGINT UNSIGNED ZEROFILL,\n"
                        + "    varchar_c            VARCHAR(255),\n"
                        + "    char_c               CHAR(3),\n"
                        + "    real_c               REAL,\n"
                        + "    float_c              FLOAT,\n"
                        + "    float_un_c           FLOAT UNSIGNED,\n"
                        + "    float_un_z_c         FLOAT UNSIGNED ZEROFILL,\n"
                        + "    double_c             DOUBLE,\n"
                        + "    double_un_c          DOUBLE UNSIGNED,\n"
                        + "    double_un_z_c        DOUBLE UNSIGNED ZEROFILL,\n"
                        + "    decimal_c            DECIMAL(8, 4),\n"
                        + "    decimal_un_c         DECIMAL(8, 4) UNSIGNED,\n"
                        + "    decimal_un_z_c       DECIMAL(8, 4) UNSIGNED ZEROFILL,\n"
                        + "    numeric_c            NUMERIC(6, 0),\n"
                        + "    big_decimal_c        DECIMAL(65, 1),\n"
                        + "    bit1_c               BIT,\n"
                        + "    tiny1_c              TINYINT(1),\n"
                        + "    boolean_c            BOOLEAN,\n"
                        + "    date_c               DATE,\n"
                        + "    time_c               TIME(0),\n"
                        + "    datetime3_c          DATETIME(3),\n"
                        + "    datetime6_c          DATETIME(6),\n"
                        + "    timestamp_c          TIMESTAMP,\n"
                        + "    file_uuid            BINARY(16),\n"
                        + "    bit_c                BIT(64),\n"
                        + "    text_c               TEXT,\n"
                        + "    tiny_text_c          tinytext,\n"
                        + "    medium_text_c        mediumtext,\n"
                        + "    long_text_c          longtext,\n"
                        + "    tiny_blob_c          TINYBLOB,\n"
                        + "    blob_c               BLOB,\n"
                        + "    medium_blob_c        MEDIUMBLOB,\n"
                        + "    long_blob_c          LONGBLOB,\n"
                        + "    year_c YEAR,\n"
                        + "    enum_c               enum('红色', 'white') default '红色',\n"
                        + "    json_c               JSON,\n"
                        + "    PRIMARY KEY (id)\n"
                        + ")";
        List<DdlOperator> operator =
                convent.rowConventToDdlData(
                        Util.mockRowDataBuilder(sql, DEFAULT_DATABASE, "c1").build());
        Assert.assertTrue(operator.size() == 1);
        Assert.assertTrue(operator.get(0) instanceof TableOperator);
        TableOperator tableOperator = (TableOperator) operator.get(0);
        Assert.assertEquals(
                "t1", tableOperator.getTableDefinition().getTableIdentifier().getTable());

        List<String> s = convent.ddlDataConventToSql(tableOperator);
        Assert.assertEquals(1, s.size());

        List<DdlOperator> definitionConvent =
                convent.rowConventToDdlData(
                        Util.mockRowDataBuilder(s.get(0), DEFAULT_DATABASE, "t1").build());

        Assert.assertEquals(1, definitionConvent.size());
        Assert.assertTrue(definitionConvent.get(0) instanceof TableOperator);
        Assert.assertEquals(EventType.CREATE_TABLE, definitionConvent.get(0).getType());

        TableOperator definitionConvent1 = (TableOperator) definitionConvent.get(0);
        Assert.assertEquals(1, definitionConvent1.getTableDefinition().getConstraintList().size());
        Assert.assertEquals(
                "id",
                definitionConvent1
                        .getTableDefinition()
                        .getConstraintList()
                        .get(0)
                        .getColumns()
                        .get(0));
        Assert.assertTrue(
                definitionConvent1.getTableDefinition().getConstraintList().get(0).isPrimary());
    }

    @Test
    public void testReplaceCreateTable() throws ConventException, SqlParseException {

        LinkedHashMap<String, String> identifierMappings = new LinkedHashMap<>();
        identifierMappings.put("dujie", "test_${dataBaseName}");
        identifierMappings.put("dujie.*", "test_${dataBaseName}.${tableName}");
        LinkedHashMap<String, String> columnTypeMappings = new LinkedHashMap<>();
        columnTypeMappings.put("int", "varchar(255)");
        MappingConfig nameMappingConfig = new MappingConfig(identifierMappings, columnTypeMappings);
        MysqlDdlConventImpl convent = new MysqlDdlConventImpl(nameMappingConfig);

        String sql = "create table  `dujie`.`t4` (id int(123) not null) ";

        List<String> map = convent.map(Util.mockRowDataBuilder(sql, "dujie", "t4").build());
        System.out.println(map.get(0));

        String sql1 = "create table  `test`.`t4` (id int(123) not null) ";

        map = convent.map(Util.mockRowDataBuilder(sql1, "test", "t4").build());
        System.out.println(map.get(0));

        map = convent.map(Util.mockRowDataBuilder("create database dujie", "dujie", null).build());
        System.out.println(map.get(0));
    }
}
