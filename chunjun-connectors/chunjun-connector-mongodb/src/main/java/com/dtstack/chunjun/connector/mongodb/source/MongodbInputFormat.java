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
import com.dtstack.chunjun.connector.mongodb.conf.MongoClientConf;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author Ada Wong
 * @program chunjun
 * @create 2021/06/21
 */
public class MongodbInputFormat extends BaseRichInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(MongodbInputFormat.class);

    private final MongoClientConf mongoClientConf;
    private final Bson filter;
    private final int fetchSize;

    private transient MongoCursor<Document> cursor;
    private transient MongoClient mongoClient;

    public MongodbInputFormat(MongoClientConf mongoClientConf, Bson filter, int fetchSize) {
        this.mongoClientConf = mongoClientConf;
        this.filter = filter;
        this.fetchSize = fetchSize;
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        ArrayList<MongodbInputSplit> splits = new ArrayList<>();

        MongoClient client = null;
        try {
            client = MongoClientFactory.createClient(mongoClientConf);
            MongoCollection<Document> collection =
                    MongoClientFactory.createCollection(
                            client, mongoClientConf.getDatabase(), mongoClientConf.getCollection());

            // 不使用 collection.countDocuments() 获取总数是因为这个方法在大数据量时超时，导致出现超时异常结束任务
            long docNum = collection.estimatedDocumentCount();
            if (docNum <= minNumSplits) {
                splits.add(new MongodbInputSplit(0, (int) docNum));
                return splits.toArray(new MongodbInputSplit[splits.size()]);
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
            LOG.error("error to create inputSplits, e = {}", ExceptionUtil.getErrorMessage(e));
            throw e;
        } finally {
            closeMongo(client, null);
        }

        return splits.toArray(new MongodbInputSplit[splits.size()]);
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("inputSplit = {}", inputSplit);
        MongodbInputSplit split = (MongodbInputSplit) inputSplit;
        FindIterable<Document> findIterable;

        mongoClient = MongoClientFactory.createClient(mongoClientConf);
        MongoCollection<Document> collection =
                MongoClientFactory.createCollection(
                        mongoClient,
                        mongoClientConf.getDatabase(),
                        mongoClientConf.getCollection());

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
    protected void closeInternal() throws IOException {
        closeMongo(mongoClient, cursor);
    }

    private void closeMongo(MongoClient mongoClient, MongoCursor<Document> cursor) {
        if (cursor != null) {
            LOG.info("Start close mongodb cursor");
            cursor.close();
            LOG.info("Close mongodb cursor successfully");
        }

        if (mongoClient != null) {
            LOG.info("Start close mongodb client");
            mongoClient.close();
            LOG.info("Close mongodb client successfully");
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !cursor.hasNext();
    }
}
