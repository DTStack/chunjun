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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlTableSpace extends SqlCall {
    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("TABLE_SPACE", SqlKind.OTHER);

    private SqlIdentifier tableSpaceName;
    private SqlNode storageTYpe;

    public SqlTableSpace(SqlParserPos pos, SqlIdentifier tableSpaceName, SqlNode storageTYpe) {
        super(pos);
        this.tableSpaceName = tableSpaceName;
        this.storageTYpe = storageTYpe;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        tableSpaceName.unparse(writer, leftPrec, rightPrec);
        if (storageTYpe != null) {
            writer.keyword("STORAGE");
            storageTYpe.unparse(writer, leftPrec, rightPrec);
        }
    }
}
