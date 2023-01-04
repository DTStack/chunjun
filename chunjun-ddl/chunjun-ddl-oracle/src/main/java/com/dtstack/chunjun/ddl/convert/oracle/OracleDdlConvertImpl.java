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

package com.dtstack.chunjun.ddl.convert.oracle;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.cdc.ddl.DdlConvent;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnOperator;
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintOperator;
import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;
import com.dtstack.chunjun.cdc.ddl.definition.IndexOperator;
import com.dtstack.chunjun.cdc.ddl.definition.TableOperator;
import com.dtstack.chunjun.ddl.convert.oracle.parse.impl.ChunjunOracleParserImpl;
import com.dtstack.chunjun.throwable.ConventException;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.Collections;
import java.util.List;

public class OracleDdlConvertImpl implements DdlConvent {

    private static final long serialVersionUID = 5228388287731540597L;

    private final SqlVisitor<List<DdlOperator>> oracleVisitor =
            new SqlNodeConvertVisitor(new SqlNodeParseImpl());

    private final OperatorConvertImpl operatorConvert = new OperatorConvertImpl();

    @Override
    public List<DdlOperator> rowConventToDdlData(DdlRowData row) throws ConventException {
        try {
            SqlParser parser = SqlParser.create(row.getSql(), getConfig());
            SqlNode sqlNode = parser.parseStmt();
            return sqlNode.accept(oracleVisitor);
        } catch (Exception e) {
            throw new ConventException(row.getSql(), e);
        }
    }

    @Override
    public List<String> ddlDataConventToSql(DdlOperator ddlOperator) throws ConventException {
        try {
            if (ddlOperator instanceof TableOperator) {
                return operatorConvert.convertOperateTable((TableOperator) ddlOperator);
            } else if (ddlOperator instanceof ColumnOperator) {
                return operatorConvert.convertOperateColumn((ColumnOperator) ddlOperator);
            } else if (ddlOperator instanceof IndexOperator) {
                return Collections.singletonList(
                        operatorConvert.convertOperateIndex((IndexOperator) ddlOperator));
            } else if (ddlOperator instanceof ConstraintOperator) {
                return Collections.singletonList(
                        operatorConvert.convertOperatorConstraint(
                                (ConstraintOperator) ddlOperator));
            }
        } catch (Exception t) {
            throw new ConventException(ddlOperator.getSql(), t);
        }
        throw new ConventException(
                ddlOperator.getSql(),
                new RuntimeException("not support convert" + ddlOperator.sql));
    }

    @Override
    public String getDataSourceType() {
        return "oracle";
    }

    @Override
    public List<String> map(DdlRowData row) {
        return Collections.singletonList(row.getSql());
    }

    public SqlParser.Config getConfig() {
        return SqlParser.config()
                // 定义解析工厂
                .withParserFactory(ChunjunOracleParserImpl.FACTORY)
                .withConformance(SqlConformanceEnum.ORACLE_12)
                .withLex(Lex.ORACLE);
    }
}
