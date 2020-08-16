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

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.mongodb.MongodbConfig;
import com.dtstack.flinkx.reader.MetaColumn;
import org.bson.json.JsonWriterSettings;

import java.util.List;

/**
 * The builder for mongodb reader plugin
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class MongodbInputFormatBuilder extends BaseRichInputFormatBuilder {

    private MongodbInputFormat format;

    public MongodbInputFormatBuilder() {
        super.format = format = new MongodbInputFormat();
    }

    public void setMetaColumns(List<MetaColumn> metaColumns){
        format.metaColumns = metaColumns;
    }

    public void setMongodbConfig(MongodbConfig mongodbConfig){
        format.mongodbConfig = mongodbConfig;
    }

    @Override
    protected void checkFormat() {
        if(format.mongodbConfig.getCollectionName() == null){
            throw new IllegalArgumentException("No collection supplied");
        }

        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
    }

    // bson转json格式设置
    public static JsonWriterSettings getJsonWriterSettings() {
        JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder()
                .dateTimeConverter((value, writer) -> writer.writeString(Long.toString(value)))
                .decimal128Converter((value, writer) -> writer.writeNumber(value.toString()))
                .objectIdConverter((value, writer) -> writer.writeString(value.toString()))
                .int32Converter((value, writer) -> writer.writeNumber(value.toString()))
                .int64Converter((value, writer) -> writer.writeString(Long.toString(value)))
                .build();

        return jsonWriterSettings;
    }


}
