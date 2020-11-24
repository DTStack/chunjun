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

package com.dtstack.flinkx.mongodb.reader;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.mongodb.MongodbClientUtil;
import com.dtstack.flinkx.mongodb.MongodbConfig;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.StringUtil;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Read plugin for reading static data
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class MongodbInputFormat extends BaseRichInputFormat {

    protected List<MetaColumn> metaColumns;

    private Bson filter;

    protected MongodbConfig mongodbConfig;

    private transient MongoCursor<Document> cursor;

    private transient MongoClient client;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        buildFilter();
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        MongodbInputSplit split = (MongodbInputSplit) inputSplit;
        FindIterable<Document> findIterable;

        client = MongodbClientUtil.getClient(mongodbConfig);
        MongoDatabase db = client.getDatabase(mongodbConfig.getDatabase());
        MongoCollection<Document> collection = db.getCollection(mongodbConfig.getCollectionName());

        if(filter == null){
            findIterable = collection.find();
        } else {
            findIterable = collection.find(filter);
        }

        findIterable = findIterable.skip(split.getSkip())
                .limit(split.getLimit())
                .batchSize(mongodbConfig.getFetchSize());
        cursor = findIterable.iterator();
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        Document doc = cursor.next();
        if(metaColumns.size() == 1 && ConstantValue.STAR_SYMBOL.equals(metaColumns.get(0).getName())){
            row = new Row(doc.size());
            String[] names = doc.keySet().toArray(new String[0]);
            for (int i = 0; i < names.length; i++) {
                Object tempData =doc.get(names[i]);
                row.setField(i, conventDocument(tempData));
            }
        } else {
            row = new Row(metaColumns.size());
            for (int i = 0; i < metaColumns.size(); i++) {
                MetaColumn metaColumn = metaColumns.get(i);

                Object value = null;
                if(metaColumn.getName() != null){
                    Object tempData = doc.get(metaColumn.getName());
                    value = conventDocument(tempData);
                    if(value == null && metaColumn.getValue() != null){
                        value = metaColumn.getValue();
                    }
                } else if(metaColumn.getValue() != null){
                    value = metaColumn.getValue();
                }

                if(value instanceof String){
                    value = StringUtil.string2col(String.valueOf(value),metaColumn.getType(),metaColumn.getTimeFormat());
                }

                row.setField(i,value);
            }
        }

        return row;
    }

    @Override
    protected void closeInternal() throws IOException {
        MongodbClientUtil.close(client, cursor);
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws IOException {
        ArrayList<MongodbInputSplit> splits = new ArrayList<>();

        MongoClient client = null;
        try {
            client = MongodbClientUtil.getClient(mongodbConfig);
            MongoDatabase db = client.getDatabase(mongodbConfig.getDatabase());
            MongoCollection<Document> collection = db.getCollection(mongodbConfig.getCollectionName());

            long docNum = filter == null ? collection.countDocuments() : collection.countDocuments(filter);
            if(docNum <= minNumSplits){
                splits.add(new MongodbInputSplit(0,(int)docNum));
                return splits.toArray(new MongodbInputSplit[splits.size()]);
            }

            long size = Math.floorDiv(docNum, minNumSplits);
            for (int i = 0; i < minNumSplits; i++) {
                splits.add(new MongodbInputSplit((int)(i * size), (int)size));
            }

            if(size * minNumSplits < docNum){
                splits.add(new MongodbInputSplit((int)(size * minNumSplits), (int)(docNum - size * minNumSplits)));
            }
        } catch (Exception e){
            LOG.error("error to create inputSplits, e = {}", ExceptionUtil.getErrorMessage(e));
            throw e;
        } finally {
            MongodbClientUtil.close(client, null);
        }

        return splits.toArray(new MongodbInputSplit[splits.size()]);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !cursor.hasNext();
    }

    private void buildFilter(){
        if(StringUtils.isNotEmpty(mongodbConfig.getFilter())){
            filter = BasicDBObject.parse(mongodbConfig.getFilter());
        }
    }

    /**
     * 如果是 map  或者 list 数据结构 使用gson转为json格式
     * 主要针对 mongodb的 Document(继承Map) 类型 ，其原有document.tostring 格式不符合正常json格式
     * @param object
     * @return
     */
    private Object conventDocument(Object object){
         if( object instanceof  List || object instanceof Map){
            return GsonUtil.GSON.toJson(object);
        }
        return object;
    }
}
