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
package com.dtstack.flinkx.cassandra;

import com.datastax.driver.core.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.cassandra.CassandraConfigKeys.*;

public class TestCassandraUtil {
    public static void main(String[] args) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(KEY_HOST, "101.37.175.174");
        configMap.put(KEY_KEY_SPACE, "tp");

        Session session = CassandraUtil.getSession(configMap, "");
        String query = "SELECT * FROM emp";
        session.execute(query);
        ResultSet result = session.execute(query);
        Iterator<Row> iterator =  result.iterator();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
            List<ColumnDefinitions.Definition> definitions = columnDefinitions.asList();
            for (ColumnDefinitions.Definition definition : definitions) {
                Object value = CassandraUtil.getData(row, definition.getType(), definition.getName());
                System.out.println(value);
            }
            System.out.println();
        }

        CassandraUtil.close(session);
    }
}
