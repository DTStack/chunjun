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

import java.io.Serializable;
import java.util.List;

public interface SqlNodeParse extends Serializable {
    long serialVersionUID = 1L;

    TableOperator parse(SqlCreateTable sqlCreate);

    TableOperator parse(SqlDropTable sqlDropTable);

    List<DdlOperator> parse(SqlRenameTable sqlRenameTable);

    List<DdlOperator> parse(SqlAlterTable sqlAlterTable);

    TableOperator parse(SqlTruncateTable sqlTruncateTable);

    IndexOperator parse(SqlDropIndex sqlDropIndex);

    IndexOperator parse(SqlCreateIndex sqlCreateIndex);
}
