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

<<<<<<< HEAD:chunjun-ddl/chunjun-ddl-mysql/src/main/java/com/dtstack/chunjun/ddl/convent/mysql/parse/enums/ReferenceOptionEnums.java
package com.dtstack.chunjun.ddl.convent.mysql.parse.enums;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;

public enum ReferenceOptionEnums {
    RESTRICT("RESTRICT"),
    CASCADE("CASCADE"),
    SET_NULL("SET NULL"),
    SET_DEFAULT("NO ACTION"),
    NO_ACTION("SET DEFAULT");

    private final String digest;

    ReferenceOptionEnums(String digest) {
        this.digest = digest;
    }

    @Override
    public String toString() {
        return digest;
    }
    /**
     * Creates a parse-tree node representing an occurrence of this keyword at a particular position
     * in the parsed text.
     */
    public SqlLiteral symbol(SqlParserPos pos) {
        return SqlLiteral.createSymbol(this, pos);
=======
package com.dtstack.chunjun.util;

import java.util.HashMap;
import java.util.Map;

public class RealTimeDataSourceNameUtil {
    // key is readerName or writerName, value is dataSourceName
    private static final Map<String, String> connectorNameMap = new HashMap<>();

    static {
        connectorNameMap.put("oraclelogminer", "oracle");
        connectorNameMap.put("binlog", "mysql");
        connectorNameMap.put("sqlservercdc", "sqlserver");
        connectorNameMap.put("pgwal", "postgresql");
    }

    public static String getDataSourceName(String pluginName) {
        String extraPluginName = PluginUtil.replaceReaderAndWriterSuffix(pluginName);
        return connectorNameMap.getOrDefault(extraPluginName, extraPluginName);
>>>>>>> origin/feat_ddl:chunjun-core/src/main/java/com/dtstack/chunjun/util/RealTimeDataSourceNameUtil.java
    }
}
