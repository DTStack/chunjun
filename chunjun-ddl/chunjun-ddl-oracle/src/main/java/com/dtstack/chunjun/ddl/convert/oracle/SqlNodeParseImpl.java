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

import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;
import com.dtstack.chunjun.cdc.ddl.definition.IndexOperator;
import com.dtstack.chunjun.cdc.ddl.definition.TableOperator;
import com.dtstack.chunjun.ddl.convert.oracle.parse.SqlAlterTable;
import com.dtstack.chunjun.ddl.convert.oracle.parse.SqlCreateIndex;
import com.dtstack.chunjun.ddl.convert.oracle.parse.SqlCreateTable;
import com.dtstack.chunjun.ddl.convert.oracle.parse.SqlDropIndex;
import com.dtstack.chunjun.ddl.convert.oracle.parse.SqlDropTable;
import com.dtstack.chunjun.ddl.convert.oracle.parse.SqlRenameTable;
import com.dtstack.chunjun.ddl.convert.oracle.parse.SqlTruncateTable;

import java.util.List;

public class SqlNodeParseImpl implements SqlNodeParse {
    @Override
    public TableOperator parse(SqlCreateTable sqlCreate) {
        return sqlCreate.parseToTableOperator();
    }

    @Override
    public TableOperator parse(SqlDropTable sqlDropTable) {
        return sqlDropTable.parseToChunJunOperator();
    }

    @Override
    public List<DdlOperator> parse(SqlRenameTable sqlRenameTable) {
        return sqlRenameTable.parseToChunjunOperator();
    }

    @Override
    public List<DdlOperator> parse(SqlAlterTable sqlAlterTable) {
        return sqlAlterTable.parseToChunjunOperator();
    }

    @Override
    public TableOperator parse(SqlTruncateTable sqlTruncateTable) {
        return sqlTruncateTable.parseToChunjunOperator();
    }

    @Override
    public IndexOperator parse(SqlDropIndex sqlDropIndex) {
        return sqlDropIndex.parseToChunjunOperator();
    }

    @Override
    public IndexOperator parse(SqlCreateIndex sqlCreateIndex) {
        return sqlCreateIndex.parseToChunjunOperator();
    }
}
