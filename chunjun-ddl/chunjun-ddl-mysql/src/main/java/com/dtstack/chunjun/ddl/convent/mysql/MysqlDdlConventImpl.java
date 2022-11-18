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

package com.dtstack.chunjun.ddl.convent.mysql;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.cdc.ddl.DdlConvent;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnOperator;
import com.dtstack.chunjun.cdc.ddl.definition.DataBaseOperator;
import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;
import com.dtstack.chunjun.cdc.ddl.definition.TableOperator;
import com.dtstack.chunjun.ddl.convent.mysql.parse.impl.ChunjunMySqlParserImpl;
import com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil;
import com.dtstack.chunjun.mapping.MappingConfig;
import com.dtstack.chunjun.mapping.MappingRule;
import com.dtstack.chunjun.throwable.ConventException;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.Collections;
import java.util.List;

public class MysqlDdlConventImpl implements DdlConvent {

    private static final long serialVersionUID = 6427759469833888308L;
    private final SqlVisitor<List<DdlOperator>> mysqlVisitor =
            new SqlNodeConventVisitor(new SqlNodeParseImpl());

    private final OperatorConvent operatorConvent = new OperatorConventImpl();
    private SqlNodeReplaceVisitor sqlNodeReplaceVisitor;

    public MysqlDdlConventImpl() {
        this(null);
    }

    public MysqlDdlConventImpl(MappingConfig mappingConfig) {
        if (null != mappingConfig) {
            this.sqlNodeReplaceVisitor = new SqlNodeReplaceVisitor(new MappingRule(mappingConfig));
        }
    }

    @Override
    public List<DdlOperator> rowConventToDdlData(DdlRowData row) throws ConventException {
        try {
            SqlParser parser = SqlParser.create(row.getSql(), getConfig());
            SqlNode sqlNode = parser.parseStmt();
            return sqlNode.accept(mysqlVisitor);
        } catch (Exception e) {
            throw new ConventException(row.getSql(), e);
        }
    }

    @Override
    public List<String> ddlDataConventToSql(DdlOperator ddlOperator) throws ConventException {
        try {
            if (ddlOperator instanceof TableOperator) {
                return Collections.singletonList(
                        operatorConvent.conventOperateTable((TableOperator) ddlOperator));
            } else if (ddlOperator instanceof DataBaseOperator) {
                return Collections.singletonList(
                        operatorConvent.conventOperateDataBase((DataBaseOperator) ddlOperator));
            } else if (ddlOperator instanceof ColumnOperator) {
                return Collections.singletonList(
                        (operatorConvent.conventOperateColumn((ColumnOperator) ddlOperator)));
            }
        } catch (Exception t) {
            throw new ConventException(ddlOperator.getSql(), t);
        }
        throw new ConventException(
                ddlOperator.getSql(),
                new RuntimeException("not support convent" + ddlOperator.sql));
    }

    @Override
    public String getDataSourceType() {
        return "mysql";
    }

    @Override
    public List<String> map(DdlRowData value) {

        if (this.sqlNodeReplaceVisitor == null) {
            return Collections.singletonList(value.getSql());
        }

        try {
            sqlNodeReplaceVisitor.setCurrentTableIdentifier(value.getTableIdentifier());
            SqlParser parser = SqlParser.create(value.getSql(), getConfig());
            SqlNode sqlNode = parser.parseStmt();
            sqlNode.accept(sqlNodeReplaceVisitor);
            return Collections.singletonList(
                    SqlNodeUtil.getSqlString(sqlNode, MysqlSqlDialect.DEFAULT));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public SqlParser.Config getConfig() {
        SqlParser.Config mysqlConfig =
                SqlParser.config()
                        // 定义解析工厂
                        .withParserFactory(ChunjunMySqlParserImpl.FACTORY)
                        .withConformance(SqlConformanceEnum.MYSQL_5)
                        .withLex(Lex.MYSQL);
        return mysqlConfig;
    }
}
