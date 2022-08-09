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

import com.dtstack.chunjun.connector.mongodb.conf.MongoClientConf;
import com.dtstack.chunjun.connector.mongodb.datasync.MongoClientConfFactory;
import com.dtstack.chunjun.connector.mongodb.datasync.MongodbDataSyncConf;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

import com.mongodb.BasicDBObject;
import org.apache.commons.lang.StringUtils;
import org.bson.conversions.Bson;

/**
 * @author Ada Wong
 * @program chunjun
 * @create 2021/06/24
 */
public class MongodbInputFormatBuilder extends BaseRichInputFormatBuilder<MongodbInputFormat> {

    public static MongodbInputFormatBuilder newBuild(MongodbDataSyncConf mongodbDataSyncConf) {
        MongoClientConf clientConf =
                MongoClientConfFactory.createMongoClientConf(mongodbDataSyncConf);
        Bson filter = parseFilter(mongodbDataSyncConf.getFilter());
        return newBuild(clientConf, filter, mongodbDataSyncConf.getFetchSize());
    }

    public static MongodbInputFormatBuilder newBuild(
            MongoClientConf mongoClientConf, Bson filter, int fetchSize) {
        MongodbInputFormat format = new MongodbInputFormat(mongoClientConf, filter, fetchSize);
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
