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

package com.dtstack.flinkx.greenplum;

import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.postgresql.PostgresqlDatabaseMeta;

import java.util.List;
import java.util.Map;

/**
 * The class of Greenplum database prototype
 *
 * @Company: www.dtstack.com
 * @author kunni@dtstack.com
 */

public class GreenplumDatabaseMeta extends PostgresqlDatabaseMeta {

    @Override
    public String getUpsertStatement(List<String> column, String table, Map<String, List<String>> updateKey) {
        throw new UnsupportedOperationException("Greenplum not support update mode");
    }

    @Override
    public EDatabaseType getDatabaseType() {
        return EDatabaseType.Greenplum;
    }

    @Override
    public String getDriverClass() {
        return "com.pivotal.jdbc.GreenplumDriver";
    }
}
