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

package com.dtstack.chunjun.ddl.convent.mysql.parse;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class KeyPart extends SqlCall {
    /** Use this operator only if you don't have a better one. */
    protected static final SqlOperator OPERATOR = new SqlSpecialOperator("keyPart", SqlKind.OTHER);

    // 一种是字符串 一种是表达式
    public SqlNode colName;
    public SqlNode columLength;
    public SqlLiteral storageOrder;

    public KeyPart(
            SqlParserPos pos, SqlNode colName, SqlNode columLength, SqlLiteral storageOrder) {
        super(pos);
        this.colName = colName;
        this.columLength = columLength;
        this.storageOrder = storageOrder;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(colName, columLength, storageOrder);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        colName.unparse(writer, leftPrec, rightPrec);
        if (columLength != null) {
            columLength.unparse(writer, leftPrec, rightPrec);
        }
        if (storageOrder != null) {
            storageOrder.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlNode getColName() {
        return colName;
    }

    public SqlNode getColumLength() {
        return columLength;
    }

    public SqlLiteral getStorageOrder() {
        return storageOrder;
    }
}
