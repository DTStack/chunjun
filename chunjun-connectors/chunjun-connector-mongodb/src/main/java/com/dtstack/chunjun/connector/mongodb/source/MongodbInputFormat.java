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

package com.dtstack.chunjun.connector.mongodb.source;

import com.dtstack.chunjun.connector.mongodb.MongoClientFactory;
import com.dtstack.chunjun.connector.mongodb.config.MongoClientConfig;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;

@Slf4j
public class MongodbInputFormat extends BaseRichInputFormat {

    private static final long serialVersionUID = -7227110247051876081L;

    private final MongoClientConfig mongoClientConfig;
    private final Bson filter;
    private final int fetchSize;

    private transient MongoCursor<Document> cursor;
    private transient MongoClient mongoClient;

    public MongodbInputFormat(MongoClientConfig mongoClientConfig, Bson filter, int fetchSize) {
        this.mongoClientConfig = mongoClientConfig;
        this.filter = filter;
        this.fetchSize = fetchSize;
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) {
        ArrayList<MongodbInputSplit> splits = new ArrayList<>();

        MongoClient client = null;
        try {
            client = MongoClientFactory.createClient(mongoClientConfig);
            MongoCollection<Document> collection =
                    MongoClientFactory.createCollection(
                            client,
                            mongoClientConfig.getDatabase(),
                            mongoClientConfig.getCollection());

            // 不使用 collection.countDocuments() 获取总数是因为这个方法在大数据量时超时，导致出现超时异常结束任务
            long docNum = collection.estimatedDocumentCount();
            if (docNum <= minNumSplits) {
                splits.add(new MongodbInputSplit(0, (int) docNum));
                return splits.toArray(new MongodbInputSplit[0]);
            }

            long size = Math.floorDiv(docNum, minNumSplits);
            for (int i = 0; i < minNumSplits; i++) {
                splits.add(new MongodbInputSplit((int) (i * size), (int) size));
            }

            if (size * minNumSplits < docNum) {
                splits.add(
                        new MongodbInputSplit(
                                (int) (size * minNumSplits), (int) (docNum - size * minNumSplits)));
            }
        } catch (Exception e) {
            log.error("error to create inputSplits, e = {}", ExceptionUtil.getErrorMessage(e));
            throw e;
        } finally {
            closeMongo(client, null);
        }

        return splits.toArray(new MongodbInputSplit[0]);
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        log.info("inputSplit = {}", inputSplit);
        MongodbInputSplit split = (MongodbInputSplit) inputSplit;
        FindIterable<Document> findIterable;

        mongoClient = MongoClientFactory.createClient(mongoClientConfig);
        MongoCollection<Document> collection =
                MongoClientFactory.createCollection(
                        mongoClient,
                        mongoClientConfig.getDatabase(),
                        mongoClientConfig.getCollection());

        if (filter == null) {
            findIterable = collection.find();
        } else {
            findIterable = collection.find(filter);
        }

        findIterable =
                findIterable.skip(split.getSkip()).limit(split.getLimit()).batchSize(fetchSize);
        cursor = findIterable.iterator();
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        Document document = cursor.next();
        try {
            return rowConverter.toInternal(document);
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
    }

    @Override
    protected void closeInternal() {
        closeMongo(mongoClient, cursor);
    }

    private void closeMongo(MongoClient mongoClient, MongoCursor<Document> cursor) {
        if (cursor != null) {
            log.info("Start close mongodb cursor");
            cursor.close();
            log.info("Close mongodb cursor successfully");
        }

        if (mongoClient != null) {
            log.info("Start close mongodb client");
            mongoClient.close();
            log.info("Close mongodb client successfully");
        }
    }

    @Override
    public boolean reachedEnd() {
        return !cursor.hasNext();
    }
}
