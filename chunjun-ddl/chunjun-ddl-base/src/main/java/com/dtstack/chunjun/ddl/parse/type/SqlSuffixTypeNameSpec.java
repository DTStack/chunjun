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

import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlSuffixTypeNameSpec extends SqlCustomTypeNameSpec {
    String nameSuffix;
    String precisionSuffix;

    public SqlSuffixTypeNameSpec(
            String name,
            String nameSuffix,
            SqlParserPos pos,
            Integer precision,
            String precisionSuffix,
            Integer scale) {
        super(name, pos, precision, scale, false, false);
        this.nameSuffix = nameSuffix;
        this.precisionSuffix = precisionSuffix;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getTypeName().getSimple());
        if (precision != null) {
            writer.print("(");
            writer.print(String.valueOf(precision));
            if (precisionSuffix != null) {
                writer.print(",");
                writer.print(precisionSuffix);
            }
            writer.print(")");
        }
        if (nameSuffix != null) {
            writer.print(nameSuffix);
        }
        if (scale != null) {
            writer.print("(");
            writer.print(String.valueOf(scale));
            writer.print(")");
        }
    }

    public String getNameSuffix() {
        return nameSuffix;
    }
}
