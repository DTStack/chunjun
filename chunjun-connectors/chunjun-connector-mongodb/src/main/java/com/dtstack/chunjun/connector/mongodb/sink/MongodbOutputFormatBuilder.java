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

package com.dtstack.chunjun.connector.mongodb.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.mongodb.conf.MongoClientConf;
import com.dtstack.chunjun.connector.mongodb.datasync.MongoClientConfFactory;
import com.dtstack.chunjun.connector.mongodb.datasync.MongodbDataSyncConf;
import com.dtstack.chunjun.sink.WriteMode;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/24
 */
public class MongodbOutputFormatBuilder extends BaseRichOutputFormatBuilder {
    MongodbDataSyncConf mongodbDataSyncConf;

    public MongodbOutputFormatBuilder(MongodbDataSyncConf mongodbDataSyncConf) {
        this.mongodbDataSyncConf = mongodbDataSyncConf;
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
    protected void checkFormat() {
        String upsertKey = mongodbDataSyncConf.getReplaceKey();
        if (!StringUtils.isBlank(upsertKey)) {
            List<FieldConf> fields = mongodbDataSyncConf.getColumn();
            boolean flag = false;
            for (FieldConf field : fields) {
                if (field.getName().equalsIgnoreCase(upsertKey)) {
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                throw new IllegalArgumentException(
                        String.format(
                                "upsertKey must be included in the column,upsertKey=[%s]",
                                upsertKey));
            }
        }
    }

    private MongodbOutputFormat.WriteMode parseWriteMode(String str) {
        if (WriteMode.REPLACE.getMode().equals(str) || WriteMode.UPDATE.getMode().equals(str)) {
            return MongodbOutputFormat.WriteMode.UPSERT;
        } else {
            return MongodbOutputFormat.WriteMode.INSERT;
        }
    }
}
