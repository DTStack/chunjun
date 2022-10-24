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

import com.dtstack.chunjun.cdc.ddl.definition.DataBaseOperator;
import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;
import com.dtstack.chunjun.cdc.ddl.definition.IndexOperator;
import com.dtstack.chunjun.cdc.ddl.definition.TableOperator;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlCreateDataBase;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlCreateIndex;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlCreateTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlDropDataBase;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlDropIndex;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlDropTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlRenameTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlTruncateTable;

import java.io.Serializable;
import java.util.List;

public interface SqlNodeParse extends Serializable {
    public static final long serialVersionUID = 1L;

    TableOperator parse(SqlCreateTable sqlCreate);

    List<TableOperator> parse(SqlDropTable sqlDropTable);

    List<TableOperator> parse(SqlRenameTable sqlRenameTable);

    List<DdlOperator> parse(SqlAlterTable sqlAlterTable);

    DataBaseOperator parse(SqlCreateDataBase sqlCreateDataBase);

    TableOperator parse(SqlTruncateTable sqlTruncateTable);

    DataBaseOperator parse(SqlDropDataBase sqlDropDataBase);

    IndexOperator parse(SqlDropIndex sqlDropIndex);

    IndexOperator parse(SqlCreateIndex sqlCreateIndex);
}
