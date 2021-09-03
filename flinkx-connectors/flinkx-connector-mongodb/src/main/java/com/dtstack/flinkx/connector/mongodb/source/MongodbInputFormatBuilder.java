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

package com.dtstack.flinkx.connector.mongodb.source;

import com.dtstack.flinkx.connector.mongodb.conf.MongoClientConf;
import com.dtstack.flinkx.connector.mongodb.datasync.MongoClientConfFactory;
import com.dtstack.flinkx.connector.mongodb.datasync.MongodbDataSyncConf;
import com.dtstack.flinkx.source.format.BaseRichInputFormatBuilder;

import com.mongodb.BasicDBObject;
import org.apache.commons.lang.StringUtils;
import org.bson.conversions.Bson;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/24
 */
public class MongodbInputFormatBuilder extends BaseRichInputFormatBuilder {

    private MongoClientConf mongoClientConf;

    public MongodbInputFormatBuilder(MongodbDataSyncConf mongodbDataSyncConf) {
        mongoClientConf = MongoClientConfFactory.createMongoClientConf(mongodbDataSyncConf);
        Bson filter = parseFilter(mongodbDataSyncConf.getFilter());
        this.format =
                new MongodbInputFormat(mongoClientConf, filter, mongodbDataSyncConf.getFetchSize());
    }

    public MongodbInputFormatBuilder(MongoClientConf mongoClientConf, Bson filter, int fetchSize) {
        this.format = new MongodbInputFormat(mongoClientConf, filter, fetchSize);
    }

    @Override
    protected void checkFormat() {}

    private Bson parseFilter(String str) {
        if (StringUtils.isNotEmpty(str)) {
            return BasicDBObject.parse(str);
        }
        return null;
    }
}
