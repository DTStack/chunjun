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

package com.dtstack.flinkx.connector.mongodb.table.lookup;

import com.dtstack.flinkx.connector.mongodb.conf.MongoClientConf;
import com.dtstack.flinkx.connector.mongodb.converter.MongodbRowConverter;
import com.dtstack.flinkx.lookup.AbstractLruTableFunction;
import com.dtstack.flinkx.lookup.conf.LookupConf;

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
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/21
 */
public class MongoLruTableFunction extends AbstractLruTableFunction {

    private static final Logger LOG = LoggerFactory.getLogger(MongoLruTableFunction.class);

    private final MongoClientConf mongoClientConf;
    private final String[] keyNames;
    private transient MongoClient mongoClient;
    private transient MongoCollection collection;

    public MongoLruTableFunction(
            MongoClientConf mongoClientConf,
            LookupConf lookupConf,
            RowType rowType,
            String[] keyNames,
            String[] fieldNames) {
        super(lookupConf, new MongodbRowConverter(rowType, fieldNames));
        this.mongoClientConf = mongoClientConf;
        this.keyNames = keyNames;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        mongoClient = MongoClients.create(new ConnectionString(mongoClientConf.getUri()));
        MongoDatabase db = mongoClient.getDatabase(mongoClientConf.getDatabase());
        collection = db.getCollection(mongoClientConf.getCollection(), Document.class);
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<RowData>> future, Object... keys)
            throws Exception {
        // 填充查询条件
        BasicDBObject basicDbObject = new BasicDBObject();
        for (int i = 0; i < keyNames.length; i++) {
            basicDbObject.append(keyNames[i], keys[i]);
        }

        AtomicInteger atomicInteger = new AtomicInteger(0);

        Block<Document> block =
                (document) -> {
                    RowData row = null;
                    try {
                        row = ((MongodbRowConverter) rowConverter).toInternalLookup(document);
                    } catch (Exception e) {
                        LOG.error("", e);
                    }
                    atomicInteger.incrementAndGet();
                    future.complete(Collections.singleton(row));
                };

        SingleResultCallback<Void> callbackWhenFinished =
                (result, t) -> {
                    if (atomicInteger.get() <= 0) {
                        LOG.warn("Cannot retrieve the data from the database");
                        future.complete(Collections.EMPTY_LIST);
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
