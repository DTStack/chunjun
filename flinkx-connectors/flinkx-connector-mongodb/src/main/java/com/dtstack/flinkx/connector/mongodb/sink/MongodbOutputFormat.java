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

package com.dtstack.flinkx.connector.mongodb.sink;

import com.dtstack.flinkx.connector.mongodb.MongoClientFactory;
import com.dtstack.flinkx.connector.mongodb.conf.MongoClientConf;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormat;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/21
 */
public class MongodbOutputFormat extends BaseRichOutputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(MongodbOutputFormat.class);

    private final MongoClientConf mongoClientConf;
    private final String key;
    private final WriteMode writeMode;

    private transient MongoClient mongoClient;
    private transient MongoCollection mongoCollection;
    private transient FindOneAndReplaceOptions options;

    public MongodbOutputFormat(MongoClientConf mongoClientConf, String key, WriteMode writeMode) {
        this.mongoClientConf = mongoClientConf;
        this.key = key;
        this.writeMode = writeMode;
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        try {
            Document document = new Document();
            rowConverter.toExternal(rowData, document);
            if (writeMode == WriteMode.UPSERT) {
                Document filter = new Document(key, document.get(key));
                mongoCollection.findOneAndReplace(filter, document, options);
            } else {
                mongoCollection.insertOne(document);
            }
        } catch (Exception e) {
            throw new WriteRecordException("Writer data to mongodb error", e, 0, rowData);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        List<Document> documents = new ArrayList<>(rows.size());
        for (RowData row : rows) {
            Document document = new Document();
            rowConverter.toExternal(row, document);
            documents.add(document);
        }
        if (writeMode == WriteMode.INSERT) {
            mongoCollection.insertMany(documents);
        } else {
            throw new FlinkxRuntimeException("Does not support batch upsert documents");
        }
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        mongoClient = MongoClientFactory.createClient(mongoClientConf);
        mongoCollection =
                MongoClientFactory.createCollection(
                        mongoClient,
                        mongoClientConf.getDatabase(),
                        mongoClientConf.getCollection());
        options = new FindOneAndReplaceOptions().upsert(true);
    }

    @Override
    protected void closeInternal() throws IOException {
        if (mongoClient != null) {
            LOG.info("Start close mongodb client");
            mongoClient.close();
            LOG.info("Close mongodb client successfully");
        }
    }

    public enum WriteMode {
        INSERT,
        UPSERT
    }
}
