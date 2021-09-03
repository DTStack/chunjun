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

import com.dtstack.flinkx.connector.mongodb.MongoClientFactory;
import com.dtstack.flinkx.connector.mongodb.conf.MongoClientConf;
import com.dtstack.flinkx.connector.mongodb.converter.MongodbRowConverter;
import com.dtstack.flinkx.lookup.AbstractAllTableFunction;
import com.dtstack.flinkx.lookup.conf.LookupConf;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.common.collect.Maps;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.util.List;
import java.util.Map;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/21
 */
public class MongoAllTableFunction extends AbstractAllTableFunction {

    private static final int FETCH_SIZE = 1000;
    private final MongoClientConf mongoClientConf;
    private transient MongoClient mongoClient;
    private transient MongoCollection collection;

    public MongoAllTableFunction(
            MongoClientConf mongoClientConf,
            LookupConf lookupConf,
            RowType rowType,
            String[] keyNames,
            String[] fieldNames) {
        super(fieldNames, keyNames, lookupConf, new MongodbRowConverter(rowType, fieldNames));
        this.mongoClientConf = mongoClientConf;
    }

    @Override
    protected void loadData(Object cacheRef) {
        mongoClient = MongoClientFactory.createClient(mongoClientConf);
        collection =
                MongoClientFactory.createCollection(
                        mongoClient,
                        mongoClientConf.getDatabase(),
                        mongoClientConf.getCollection());
        Map<String, List<Map<String, Object>>> tmpCache =
                (Map<String, List<Map<String, Object>>>) cacheRef;

        FindIterable<Document> findIterable = collection.find().limit(FETCH_SIZE);
        MongoCursor<Document> mongoCursor = findIterable.iterator();
        while (mongoCursor.hasNext()) {
            Document doc = mongoCursor.next();
            Map<String, Object> row = Maps.newHashMap();
            GenericRowData rowData =
                    (GenericRowData) ((MongodbRowConverter) rowConverter).toInternal(doc);
            for (int i = 0; i < fieldsName.length; i++) {
                Object object = rowData.getField(i);
                row.put(fieldsName[i].trim(), object);
            }
            buildCache(row, tmpCache);
        }
    }
}
