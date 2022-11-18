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

package com.dtstack.chunjun.connector.cassandra.lookup;

import com.dtstack.chunjun.connector.cassandra.config.CassandraCommonConfig;
import com.dtstack.chunjun.connector.cassandra.config.CassandraLookupConfig;
import com.dtstack.chunjun.connector.cassandra.util.CassandraService;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.lookup.AbstractAllTableFunction;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.functions.FunctionContext;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.chunjun.connector.cassandra.util.CassandraService.quoteColumn;

@Slf4j
public class CassandraAllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = -7838106494289424452L;

    private final CassandraLookupConfig cassandraLookupConfig;

    private transient Session session;

    public CassandraAllTableFunction(
            CassandraLookupConfig lookupConfig,
            AbstractRowConverter<?, ?, ?, ?> rowConverter,
            String[] fieldNames,
            String[] keyNames) {
        super(fieldNames, keyNames, lookupConfig, rowConverter);
        this.cassandraLookupConfig = lookupConfig;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    @Override
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache =
                (Map<String, List<Map<String, Object>>>) cacheRef;
        CassandraCommonConfig commonConfig = cassandraLookupConfig.getCommonConfig();

        session = CassandraService.session(commonConfig);

        String keyspaces = commonConfig.getKeyspaces();
        String tableName = commonConfig.getTableName();

        List<String> quotedColumnNameList = new ArrayList<>();
        Arrays.stream(fieldsName).forEach(name -> quotedColumnNameList.add(quoteColumn(name)));

        Select select =
                QueryBuilder.select(quotedColumnNameList.toArray(new String[0]))
                        .from(keyspaces, tableName);

        ResultSet resultSet = session.execute(select);

        for (Row row : resultSet) {
            Map<String, Object> oneRow = new HashMap<>();
            // 防止一条数据有问题，后面数据无法加载
            try {
                GenericRowData rowData = (GenericRowData) rowConverter.toInternalLookup(row);
                for (int i = 0; i < fieldsName.length; i++) {
                    Object object = rowData.getField(i);
                    oneRow.put(fieldsName[i].trim(), object);
                }
                buildCache(oneRow, tmpCache);
            } catch (Exception e) {
                log.error("", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();

        CassandraService.close(session);
    }
}
