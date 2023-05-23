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

import com.dtstack.chunjun.connector.mongodb.config.MongoClientConfig;
import com.dtstack.chunjun.connector.mongodb.converter.MongodbSqlConverter;
import com.dtstack.chunjun.lookup.AbstractLruTableFunction;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.ConnectionString;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public class MongoLruTableFunction extends AbstractLruTableFunction {

    private static final long serialVersionUID = -7492650828362052066L;

    private final MongoClientConfig mongoClientConfig;
    private final String[] keyNames;
    private transient MongoClient mongoClient;
    private transient MongoCollection<Document> collection;

    public MongoLruTableFunction(
            MongoClientConfig mongoClientConfig,
            LookupConfig lookupConfig,
            RowType rowType,
            String[] keyNames,
            String[] fieldNames) {
        super(lookupConfig, new MongodbSqlConverter(rowType, fieldNames));
        this.mongoClientConfig = mongoClientConfig;
        this.keyNames = keyNames;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        mongoClient = MongoClients.create(new ConnectionString(mongoClientConfig.getUri()));
        MongoDatabase db = mongoClient.getDatabase(mongoClientConfig.getDatabase());
        collection = db.getCollection(mongoClientConfig.getCollection(), Document.class);
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<RowData>> future, Object... keys)
            throws Exception {
        // 填充查询条件
        BasicDBObject basicDbObject = new BasicDBObject();
        for (int i = 0; i < keyNames.length; i++) {
            basicDbObject.append(keyNames[i], keys[i]);
        }

        List<RowData> rowList = new CopyOnWriteArrayList<>();

        Block<Document> block =
                (document) -> {
                    RowData row = null;
                    try {
                        row = ((MongodbSqlConverter) rowConverter).toInternalLookup(document);
                    } catch (Exception e) {
                        log.error("", e);
                    }
                    rowList.add(row);
                };

        SingleResultCallback<Void> callbackWhenFinished =
                (result, t) -> {
                    if (rowList.isEmpty()) {
                        log.warn("Cannot retrieve the data from the database");
                        future.complete(Collections.emptyList());
                    } else {
                        future.complete(rowList);
                    }
                };

        collection.find(basicDbObject).forEach(block, callbackWhenFinished);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
