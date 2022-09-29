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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlCustomTypeNameSpec extends SqlUserDefinedTypeNameSpec {
    public final Integer precision;
    public final Integer scale;
    public final boolean isUnsigned;
    public final boolean isZeroFill;

    public SqlCustomTypeNameSpec(
            SqlIdentifier typeName,
            SqlParserPos pos,
            Integer precision,
            Integer scale,
            boolean isUnsigned,
            boolean isZeroFill) {
        super(typeName, pos);
        this.precision = precision;
        this.scale = scale;
        this.isUnsigned = isUnsigned;
        this.isZeroFill = isZeroFill;
    }

    public SqlCustomTypeNameSpec(
            String name,
            SqlParserPos pos,
            Integer precision,
            Integer scale,
            boolean isUnsigned,
            boolean isZeroFill) {
        super(name, pos);
        this.precision = precision;
        this.scale = scale;
        this.isUnsigned = isUnsigned;
        this.isZeroFill = isZeroFill;
    }

    public SqlCustomTypeNameSpec(String name, SqlParserPos pos) {
        super(name, pos);
        this.precision = null;
        this.scale = null;
        this.isUnsigned = false;
        this.isZeroFill = false;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getTypeName().getSimple());
        if (precision != null) {
            writer.print("(");
            writer.print(String.valueOf(precision));
            if (scale != null) {
                writer.print(",");
                writer.print(String.valueOf(precision));
            }
            writer.print(")");
        }

        if (isUnsigned) {
            writer.keyword("UNSIGNED");
        }
        if (isZeroFill) {
            writer.keyword("ZEROFILL");
        }
    }
}
