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

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.mongodb.config.MongoClientConfig;
import com.dtstack.chunjun.connector.mongodb.datasync.MongoClientConfFactory;
import com.dtstack.chunjun.connector.mongodb.datasync.MongodbDataSyncConfig;
import com.dtstack.chunjun.sink.WriteMode;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class MongodbOutputFormatBuilder extends BaseRichOutputFormatBuilder<MongodbOutputFormat> {
    MongodbDataSyncConfig mongodbDataSyncConfig;
    String upsertKey;

    public static MongodbOutputFormatBuilder newBuilder(
            MongodbDataSyncConfig mongodbDataSyncConfig) {
        String upsertKey = mongodbDataSyncConfig.getReplaceKey();
        MongoClientConfig mongoClientConfig =
                MongoClientConfFactory.createMongoClientConf(mongodbDataSyncConfig);
        MongodbOutputFormat.WriteMode writeMode =
                parseWriteMode(mongodbDataSyncConfig.getWriteMode());
        return new MongodbOutputFormatBuilder(
                mongodbDataSyncConfig, mongoClientConfig, upsertKey, writeMode);
    }

    public MongodbOutputFormatBuilder(
            MongodbDataSyncConfig mongodbDataSyncConfig,
            MongoClientConfig mongoClientConfig,
            String key,
            MongodbOutputFormat.WriteMode writeMode) {
        super(new MongodbOutputFormat(mongoClientConfig, key, writeMode));
        this.upsertKey = key;
        this.mongodbDataSyncConfig = mongodbDataSyncConfig;
    }

    @Override
    protected void checkFormat() {
        // mongodbDataSyncConf 是json模式下的实体类
        // sql 模式这里跳过检查
        if (mongodbDataSyncConfig == null) {
            return;
        }
        if (!StringUtils.isBlank(upsertKey)) {
            List<FieldConfig> fields = mongodbDataSyncConfig.getColumn();
            boolean flag = false;
            for (FieldConfig field : fields) {
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

    private static MongodbOutputFormat.WriteMode parseWriteMode(String str) {
        if (WriteMode.REPLACE.getMode().equals(str) || WriteMode.UPDATE.getMode().equals(str)) {
            return MongodbOutputFormat.WriteMode.UPSERT;
        } else {
            return MongodbOutputFormat.WriteMode.INSERT;
        }
    }
}
