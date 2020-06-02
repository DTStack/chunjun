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

package com.dtstack.flinkx.mongodb.writer;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.mongodb.MongodbClientUtil;
import com.dtstack.flinkx.mongodb.MongodbConfig;
import com.dtstack.flinkx.mongodb.MongodbUtil;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.writer.WriteMode;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * OutputFormat for mongodb writer plugin
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class MongodbOutputFormat extends BaseRichOutputFormat {

    protected List<MetaColumn> columns;

    private transient MongoCollection<Document> collection;

    private transient MongoClient client;

    protected MongodbConfig mongodbConfig;

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        client = MongodbClientUtil.getClient(mongodbConfig);
        MongoDatabase db = client.getDatabase(mongodbConfig.getDatabase());
        collection = db.getCollection(mongodbConfig.getCollectionName());
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        try {
            Document doc = MongodbUtil.convertRowToDoc(row,columns);

            if(WriteMode.INSERT.getMode().equals(mongodbConfig.getWriteMode())){
                collection.insertOne(doc);
            } else if(WriteMode.REPLACE.getMode().equals(mongodbConfig.getWriteMode())
                    || WriteMode.UPDATE.getMode().equals(mongodbConfig.getWriteMode())){
                Document filter = new Document(mongodbConfig.getReplaceKey(), doc.get(mongodbConfig.getReplaceKey()));
                collection.findOneAndReplace(filter,doc);
            }
        } catch (Exception e){
            throw new WriteRecordException("Writer data to mongodb error", e, 0, row);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        List<Document> documents = new ArrayList<>(rows.size());
        for (Row row : rows) {
            documents.add(MongodbUtil.convertRowToDoc(row,columns));
        }

        if(WriteMode.INSERT.getMode().equals(mongodbConfig.getWriteMode())){
            collection.insertMany(documents);
        } else if(WriteMode.UPDATE.getMode().equals(mongodbConfig.getWriteMode())) {
            throw new RuntimeException("Does not support batch update documents");
        } else if(WriteMode.REPLACE.getMode().equals(mongodbConfig.getWriteMode())){
            throw new RuntimeException("Does not support batch replace documents");
        }
    }

    @Override
    public void closeInternal() throws IOException {
        MongodbClientUtil.close(client, null);
    }
}
