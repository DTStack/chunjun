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


package com.dtstack.mongodb.oplog.reader;

import org.apache.flink.types.Row;
import org.bson.BsonTimestamp;
import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jiangbo
 * @date 2019/12/5
 */
public class MongodbEventHandler {

    public final static String EVENT_KEY_OP = "op";
    public final static String EVENT_KEY_NS = "ns";
    public final static String EVENT_KEY_TS = "ts";
    public final static String EVENT_KEY_DATA = "o";

    public static Row handleEvent(final Document event, AtomicLong offset, boolean excludeDocId){
        MongodbOperation mongodbOperation = MongodbOperation.getByInternalNames(event.getString(EVENT_KEY_OP));
        Map<String, Object> eventMap = new LinkedHashMap<>();
        eventMap.put("type", mongodbOperation.name());

        parseDbAndCollection(event, eventMap);

        BsonTimestamp timestamp = event.get(EVENT_KEY_TS, BsonTimestamp.class);
        eventMap.put("ts", timestamp.getValue());
        eventMap.put("ingestion", System.nanoTime());

        final Document data = (Document)event.get(EVENT_KEY_DATA);
        Set<String> keys = data.keySet();
        if(excludeDocId){
            keys.remove("_id");
        }

        for (String key : keys) {
            eventMap.put("after_" + key, data.get(key));
        }

        for (String key : keys) {
            eventMap.put("before_" + key, null);
        }

        offset.set(timestamp.getValue());
        return Row.of(eventMap);
    }

    private static void parseDbAndCollection(final Document event, Map<String, Object> eventMap){
        String dbCollection = event.getString(EVENT_KEY_NS);
        String[] split = dbCollection.split("\\.");
        eventMap.put("schema", split[0]);
        eventMap.put("table", split[1]);
    }
}
