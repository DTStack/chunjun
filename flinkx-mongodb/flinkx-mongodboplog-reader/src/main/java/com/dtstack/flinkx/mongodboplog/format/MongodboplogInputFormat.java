/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.dtstack.flinkx.mongodboplog.format;

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.mongodb.MongodbClientUtil;
import com.dtstack.flinkx.mongodb.MongodbConfig;
import com.dtstack.flinkx.restore.FormatState;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jiangbo
 * @date 2019/12/5
 */
public class MongodboplogInputFormat extends BaseRichInputFormat {

    private final static String OPLOG_DB = "local";
    private final static String REPLICA_SET_COLLECTION = "oplog.rs";
    private final static String MASTER_SLAVE_COLLECTION = "oplog.$main";

    protected MongodbConfig mongodbConfig;

    private transient MongoClient client;

    private transient MongoCursor<Document> cursor;

    private AtomicLong offset = new AtomicLong();

    private InputSplit inputSplit;

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        this.inputSplit = inputSplit;
        initOffset();

        client = MongodbClientUtil.getClient(mongodbConfig);
        MongoCollection<Document> oplog = getOplogCollection();
        FindIterable<Document> results = oplog.find(buildFilter())
                .sort(new Document("$natural", 1))
                .oplogReplay(true)
                .cursorType(CursorType.TailableAwait);

        cursor = results.iterator();
    }

    /**
     * 在 master/slave 结构下, oplog 位于local.oplog.$main
     * 在 Replca set 结构下， oplog 位于local.oplog.rs
     */
    private MongoCollection<Document> getOplogCollection(){
        if ("REPLICA_SET".equalsIgnoreCase(mongodbConfig.getClusterMode())) {
            return client.getDatabase(OPLOG_DB).getCollection(REPLICA_SET_COLLECTION);
        } else if("MASTER_SLAVE".equalsIgnoreCase(mongodbConfig.getClusterMode())){
            return client.getDatabase(OPLOG_DB).getCollection(MASTER_SLAVE_COLLECTION);
        } else {
            throw new RuntimeException("集群模式不支持:" + mongodbConfig.getClusterMode());
        }
    }

    private void initOffset(){
        BsonTimestamp startLocation = new BsonTimestamp(mongodbConfig.getStartLocation(), 0);
        if (formatState != null && formatState.getState() != null) {
            offset.set(Long.valueOf(formatState.getState().toString()));
            long state = (Long)formatState.getState();
            if (startLocation.compareTo(new BsonTimestamp(state)) > 0) {
                offset.set(mongodbConfig.getStartLocation());
            } else {
                offset.set(state);
            }
        } else {
            offset.set(startLocation.getValue());
        }
    }

    private Bson buildFilter(){
        List<Bson> filters = new ArrayList<>();

        // 设置读取位置
        filters.add(Filters.gt(MongodbEventHandler.EVENT_KEY_TS, new BsonTimestamp(offset.get())));

        //
        filters.add(Filters.exists("fromMigrate", false));

        // 过滤db和collection
        String pattern = buildPattern();
        if (pattern != null) {
            filters.add(Filters.regex(MongodbEventHandler.EVENT_KEY_NS, pattern));
        }

        // 过滤系统日志
        filters.add(Filters.ne(MongodbEventHandler.EVENT_KEY_NS, "config.system.sessions"));

        // 过滤操作类型
        if(CollectionUtils.isNotEmpty(mongodbConfig.getOperateType())) {
            List<String> operateTypes = MongodbOperation.getInternalNames(mongodbConfig.getOperateType());
            filters.add(Filters.in(MongodbEventHandler.EVENT_KEY_OP, operateTypes));
        }

        return Filters.and(filters);
    }

    private String buildPattern() {
        if (CollectionUtils.isEmpty(mongodbConfig.getMonitorDatabases()) && CollectionUtils.isEmpty(mongodbConfig.getMonitorCollections())){
            return null;
        }

        StringBuilder pattern = new StringBuilder();
        if(CollectionUtils.isNotEmpty(mongodbConfig.getMonitorDatabases())){
            mongodbConfig.getMonitorDatabases().removeIf(StringUtils::isEmpty);
            if(CollectionUtils.isNotEmpty(mongodbConfig.getMonitorDatabases())){
                String databasePattern = StringUtils.join(mongodbConfig.getMonitorDatabases(), "|");
                pattern.append("(").append(databasePattern).append(")");
            } else {
                pattern.append(".*");
            }
        }

        pattern.append("\\.");

        if(CollectionUtils.isNotEmpty(mongodbConfig.getMonitorCollections())){
            mongodbConfig.getMonitorCollections().removeIf(String::isEmpty);
            if(CollectionUtils.isNotEmpty(mongodbConfig.getMonitorCollections())){
                String collectionPattern = StringUtils.join(mongodbConfig.getMonitorCollections(), "|");
                pattern.append("(").append(collectionPattern).append(")");
            } else {
                pattern.append(".*");
            }
        }

        return pattern.toString();
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        return MongodbEventHandler.handleEvent(cursor.next(), offset, mongodbConfig.getExcludeDocId(), mongodbConfig.getPavingData());
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();

        if (formatState != null){
            formatState.setState(offset.get());
        }

        return formatState;
    }

    @Override
    protected void closeInternal() throws IOException {
        MongodbClientUtil.close(client, cursor);
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws IOException {
        return new InputSplit[]{new GenericInputSplit(1,1)};
    }

    @Override
    public boolean reachedEnd() throws IOException {
        try {
            return !cursor.hasNext();
        } catch (Exception e) {
            // 这里出现异常可能是因为集群里某个节点挂了，所以不退出程序，调用openInternal方法重新连接，并从offset处开始同步数据，
            // 如果集群有问题，在openInternal方法里结束进程
            LOG.warn("获取数据异常,可能是某个节点出问题了，程序将自动重新选择节点连接", e);
            closeInternal();
            openInternal(inputSplit);

            return false;
        }
    }
}
