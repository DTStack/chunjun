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

import com.dtstack.chunjun.connector.mongodb.config.MongoClientConfig;
import com.dtstack.chunjun.connector.mongodb.datasync.MongoClientConfFactory;
import com.dtstack.chunjun.connector.mongodb.datasync.MongodbDataSyncConfig;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

import com.mongodb.BasicDBObject;
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;

public class MongodbInputFormatBuilder extends BaseRichInputFormatBuilder<MongodbInputFormat> {

    public static MongodbInputFormatBuilder newBuild(MongodbDataSyncConfig mongodbDataSyncConfig) {
        MongoClientConfig clientConf =
                MongoClientConfFactory.createMongoClientConf(mongodbDataSyncConfig);
        Bson filter = parseFilter(mongodbDataSyncConfig.getFilter());
        return newBuild(clientConf, filter, mongodbDataSyncConfig.getFetchSize());
    }

    public static MongodbInputFormatBuilder newBuild(
            MongoClientConfig mongoClientConfig, Bson filter, int fetchSize) {
        MongodbInputFormat format = new MongodbInputFormat(mongoClientConfig, filter, fetchSize);
        return new MongodbInputFormatBuilder(format);
    }

    public static MongodbInputFormatBuilder newBuild(
            MongoClientConfig mongoClientConf, String filter, int fetchSize) {
        MongodbInputFormat format =
                new MongodbInputFormat(mongoClientConf, parseFilter(filter), fetchSize);
        return new MongodbInputFormatBuilder(format);
    }

    private MongodbInputFormatBuilder(MongodbInputFormat format) {
        super(format);
    }

    @Override
    protected void checkFormat() {}

    private static Bson parseFilter(String str) {
        if (StringUtils.isNotEmpty(str)) {
            return BasicDBObject.parse(str);
        }
        return null;
    }
}
