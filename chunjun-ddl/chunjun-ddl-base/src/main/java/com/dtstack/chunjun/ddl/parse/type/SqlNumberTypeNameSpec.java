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

package com.dtstack.chunjun.ddl.parse.type;

import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

public class SqlNumberTypeNameSpec extends SqlBasicTypeNameSpec {

    public final boolean isUnsigned;
    public final boolean isZeroFill;

    public SqlNumberTypeNameSpec(
            SqlTypeName typeName, SqlParserPos pos, boolean isUnsigned, boolean isZeroFill) {
        super(typeName, pos);
        this.isUnsigned = isUnsigned;
        this.isZeroFill = isZeroFill;
    }

    public SqlNumberTypeNameSpec(
            SqlTypeName typeName,
            int precision,
            int scale,
            boolean isUnsigned,
            boolean isZeroFill,
            SqlParserPos pos) {
        super(typeName, precision, scale, pos);
        this.isUnsigned = isUnsigned;
        this.isZeroFill = isZeroFill;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        if (isUnsigned) {
            writer.keyword("UNSIGNED");
        }
        if (isZeroFill) {
            writer.keyword("ZEROFILL");
        }
    }
}
