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

import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlCreateDataBase;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlCreateIndex;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlCreateTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlDropDataBase;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlDropIndex;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlDropTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlRenameTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlTruncateTable;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlVisitor;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class SqlNodeConventVisitor implements SqlVisitor<List<DdlOperator>>, Serializable {
    public static final long serialVersionUID = 1L;

    private final SqlNodeParse sqlNodeParse;

    public SqlNodeConventVisitor(SqlNodeParse sqlNodeParse) {
        this.sqlNodeParse = sqlNodeParse;
    }

    @Override
    public List<DdlOperator> visit(SqlLiteral literal) {
        return null;
    }

    @Override
    public List<DdlOperator> visit(SqlCall call) {

        if (call instanceof SqlCreateTable) {
            return Collections.singletonList(sqlNodeParse.parse((SqlCreateTable) call));
        } else if (call instanceof SqlCreateDataBase) {
            return Collections.singletonList(sqlNodeParse.parse((SqlCreateDataBase) call));
        } else if (call instanceof SqlDropDataBase) {
            return Collections.singletonList(sqlNodeParse.parse((SqlDropDataBase) call));
        } else if (call instanceof SqlDropTable) {
            return Lists.newArrayList(sqlNodeParse.parse((SqlDropTable) call));
        } else if (call instanceof SqlTruncateTable) {
            return Lists.newArrayList(sqlNodeParse.parse((SqlTruncateTable) call));
        } else if (call instanceof SqlRenameTable) {
            return Lists.newArrayList(sqlNodeParse.parse((SqlRenameTable) call));
        } else if (call instanceof SqlAlterTable) {
            return Lists.newArrayList(sqlNodeParse.parse((SqlAlterTable) call));
        } else if (call instanceof SqlDropIndex) {
            return Collections.singletonList(sqlNodeParse.parse((SqlDropIndex) call));
        } else if (call instanceof SqlCreateIndex) {
            return Collections.singletonList(sqlNodeParse.parse((SqlCreateIndex) call));
        }
        // ...... more
        throw new UnsupportedOperationException("unSupport parse sql " + call.toString());
    }

    @Override
    public List<DdlOperator> visit(SqlNodeList nodeList) {
        return null;
    }

    @Override
    public List<DdlOperator> visit(SqlIdentifier id) {
        return null;
    }

    @Override
    public List<DdlOperator> visit(SqlDataTypeSpec type) {
        return null;
    }

    @Override
    public List<DdlOperator> visit(SqlDynamicParam param) {
        return null;
    }

    @Override
    public List<DdlOperator> visit(SqlIntervalQualifier intervalQualifier) {
        return null;
    }
}
