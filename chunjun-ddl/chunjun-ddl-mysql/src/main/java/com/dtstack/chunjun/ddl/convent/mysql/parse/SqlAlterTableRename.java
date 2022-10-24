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

import com.dtstack.chunjun.ddl.convent.mysql.parse.enums.AlterTableRenameTypeEnum;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class SqlAlterTableRename extends SqlAlterTableOperator {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("ALTER TABLE CHANGE COLUMN", SqlKind.ALTER_TABLE);

    public SqlIdentifier oldName;
    public SqlIdentifier newName;
    public SqlLiteral renameType;

    public SqlAlterTableRename(
            SqlParserPos pos,
            SqlIdentifier tableIdentifier,
            SqlIdentifier oldName,
            SqlIdentifier newName,
            SqlLiteral renameType) {
        super(pos, tableIdentifier);
        this.oldName = oldName;
        this.newName = newName;
        this.renameType = renameType;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableIdentifier, oldName, newName, renameType);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER TABLE");
        tableIdentifier.unparse(writer, leftPrec, rightPrec);
        writer.keyword("RENAME");
        if (renameType.toValue().equals(AlterTableRenameTypeEnum.COLUMN.name())) {
            writer.keyword("COLUMN");
            oldName.unparse(writer, leftPrec, rightPrec);
        } else if (renameType.toValue().equals(AlterTableRenameTypeEnum.INDEX.name())) {
            writer.keyword("INDEX");
            oldName.unparse(writer, leftPrec, rightPrec);
        } else if (renameType.toValue().equals(AlterTableRenameTypeEnum.KEY.name())) {
            writer.keyword("KEY");
            oldName.unparse(writer, leftPrec, rightPrec);
        }
        writer.keyword("TO");

        newName.unparse(writer, leftPrec, rightPrec);
    }
}
