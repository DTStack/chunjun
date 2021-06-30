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

import com.dtstack.flinkx.connector.mongodb.conf.MongoClientConf;
import com.dtstack.flinkx.connector.mongodb.datasync.MongoClientConfFactory;
import com.dtstack.flinkx.connector.mongodb.datasync.MongodbDataSyncConf;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import com.dtstack.flinkx.sink.WriteMode;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/24
 */
public class MongodbOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    public MongodbOutputFormatBuilder(MongodbDataSyncConf mongodbDataSyncConf) {
        MongoClientConf mongoClientConf =
                MongoClientConfFactory.createMongoClientConf(mongodbDataSyncConf);
        MongodbOutputFormat.WriteMode writeMode =
                parseWriteMode(mongodbDataSyncConf.getWriteMode());
        this.format =
                new MongodbOutputFormat(
                        mongoClientConf, mongodbDataSyncConf.getReplaceKey(), writeMode);
    }

    public MongodbOutputFormatBuilder(
            MongoClientConf mongoClientConf, String key, MongodbOutputFormat.WriteMode writeMode) {
        this.format = new MongodbOutputFormat(mongoClientConf, key, writeMode);
    }

    @Override
    protected void checkFormat() {}

    private MongodbOutputFormat.WriteMode parseWriteMode(String str) {
        if (WriteMode.REPLACE.getMode().equals(str) || WriteMode.UPDATE.getMode().equals(str)) {
            return MongodbOutputFormat.WriteMode.UPSERT;
        } else {
            return MongodbOutputFormat.WriteMode.INSERT;
        }
    }
}
