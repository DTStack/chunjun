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

package com.dtstack.chunjun.ddl.convert.oracle.parse;

import com.dtstack.chunjun.annotation.NotNull;
import com.dtstack.chunjun.cdc.ddl.ColumnTypeConvert;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnDefinition;
import com.dtstack.chunjun.ddl.parse.type.SqlSuffixTypeNameSpec;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.getSqlString;
import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.getTypePrecisionAndScale;
import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.unparseSqlCharStringLiteral;

public class SqlGeneralColumn extends SqlCall {
    private final SqlIdentifier name;
    private final SqlDataTypeSpec type;
    private final SqlNode defaultValue;

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);

    public SqlGeneralColumn(
            SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec type, SqlNode defaultValue) {
        super(pos);
        this.name = name;
        this.type = type;
        this.defaultValue = defaultValue;
    }

    @NotNull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @NotNull
    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        name.unparse(writer, leftPrec, rightPrec);
        type.unparse(writer, leftPrec, rightPrec);
        if (defaultValue != null) {
            writer.keyword(" DEFAULT ");
            if (!unparseSqlCharStringLiteral(defaultValue, writer, leftPrec, rightPrec)) {
                defaultValue.unparse(writer, leftPrec, rightPrec);
            }
        }
    }

    private void unparseNode(SqlNode node, SqlWriter writer, int leftPrec, int rightPrec) {
        if (node != null) {
            writer.keyword(" ");
            node.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlDataTypeSpec getType() {
        return type;
    }

    public SqlNode getDefaultValue() {
        return defaultValue;
    }

    public ColumnDefinition parseToChunjunOperator(ColumnTypeConvert convert) {
        String typeName;
        SqlTypeNameSpec typeNameSpec = type.getTypeNameSpec();
        if (typeNameSpec instanceof SqlSuffixTypeNameSpec) {
            typeName =
                    typeNameSpec.getTypeName().getSimple()
                            + ((SqlSuffixTypeNameSpec) typeNameSpec).getNameSuffix();
        } else {
            typeName = typeNameSpec.getTypeName().getSimple();
        }
        Pair<Integer, Integer> typePrecisionAndScale = getTypePrecisionAndScale(typeNameSpec);

        String defaultValueStr = null;
        if (defaultValue != null) {
            defaultValueStr = getSqlString(defaultValue);
        }

        return new ColumnDefinition.Builder()
                .name(getSqlString(name))
                .type(convert.conventDataSourceTypeToColumnType(typeName))
                .precision(typePrecisionAndScale.getLeft())
                .scale(typePrecisionAndScale.getRight())
                .defaultValue(defaultValueStr)
                .build();
    }
}
