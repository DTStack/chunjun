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

package com.dtstack.flinkx.mongodb.writer;

import com.dtstack.flinkx.mongodb.MongodbConfig;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.writer.WriteMode;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * The builder for mongodb writer plugin
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class MongodbOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private MongodbOutputFormat format;

    public MongodbOutputFormatBuilder() {
        super.format = format = new MongodbOutputFormat();
    }

    public void setMongodbConfig(MongodbConfig mongodbConfig){
        format.mongodbConfig = mongodbConfig;
    }

    public void setColumns(List<MetaColumn> columns){
        format.columns = columns;
    }

    @Override
    protected void checkFormat() {
        if(format.mongodbConfig.getCollectionName() == null){
            throw new IllegalArgumentException("No collection supplied");
        }

        if(WriteMode.REPLACE.getMode().equals(format.mongodbConfig.getWriteMode())
                || WriteMode.UPDATE.getMode().equals(format.mongodbConfig.getWriteMode())){
            if(StringUtils.isEmpty(format.mongodbConfig.getReplaceKey())){
                throw new IllegalArgumentException("ReplaceKey cannot be empty when the write mode is replace");
            }

            boolean columnContainsReplaceKey = false;
            for (MetaColumn column : format.columns) {
                if (column.getName().equalsIgnoreCase(format.mongodbConfig.getReplaceKey())) {
                    columnContainsReplaceKey = true;
                    break;
                }
            }

            if(!columnContainsReplaceKey){
                throw new IllegalArgumentException("Cannot find replaceKey in the input fields");
            }
        }

        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
    }
}
