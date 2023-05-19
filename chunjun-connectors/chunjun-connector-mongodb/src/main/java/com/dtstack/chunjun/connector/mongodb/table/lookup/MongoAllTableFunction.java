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

package com.dtstack.chunjun.connector.mongodb.table.lookup;

import com.dtstack.chunjun.connector.mongodb.MongoClientFactory;
import com.dtstack.chunjun.connector.mongodb.config.MongoClientConfig;
import com.dtstack.chunjun.connector.mongodb.converter.MongodbSqlConverter;
import com.dtstack.chunjun.lookup.AbstractAllTableFunction;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.common.collect.Maps;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.List;
import java.util.Map;

public class MongoAllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = -2554848099170846741L;

    private final int fetchSize;
    private final MongoClientConfig mongoClientConfig;

    public MongoAllTableFunction(
            MongoClientConfig mongoClientConfig,
            LookupConfig lookupConfig,
            RowType rowType,
            String[] keyNames,
            String[] fieldNames) {
        super(fieldNames, keyNames, lookupConfig, new MongodbSqlConverter(rowType, fieldNames));
        this.mongoClientConfig = mongoClientConfig;
        this.fetchSize = lookupConfig.getFetchSize();
    }

    @Override
    protected void loadData(Object cacheRef) {
        MongoClient mongoClient = MongoClientFactory.createClient(mongoClientConfig);
        MongoCollection<Document> collection =
                MongoClientFactory.createCollection(
                        mongoClient,
                        mongoClientConfig.getDatabase(),
                        mongoClientConfig.getCollection());
        Map<String, List<Map<String, Object>>> tmpCache =
                (Map<String, List<Map<String, Object>>>) cacheRef;

        FindIterable<Document> findIterable = collection.find().limit(fetchSize);
        for (Document doc : findIterable) {
            Map<String, Object> row = Maps.newHashMap();
            GenericRowData rowData =
                    (GenericRowData) ((MongodbSqlConverter) rowConverter).toInternal(doc);
            for (int i = 0; i < fieldsName.length; i++) {
                Object object = rowData.getField(i);
                row.put(fieldsName[i].trim(), object);
            }
            buildCache(row, tmpCache);
        }
    }
}
