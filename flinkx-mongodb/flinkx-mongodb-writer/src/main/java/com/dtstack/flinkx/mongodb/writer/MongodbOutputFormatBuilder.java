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

import com.dtstack.flinkx.outputformat.RichOutputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;

import java.util.List;
import java.util.Map;


/**
 * The builder for mongodb writer plugin
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class MongodbOutputFormatBuilder extends RichOutputFormatBuilder {

    private MongodbOutputFormat format;

    public MongodbOutputFormatBuilder() {
        super.format = format = new MongodbOutputFormat();
    }

    public void setHostPorts(String hostPorts){
        format.hostPorts = hostPorts;
    }

    public void setUsername(String username){
        format.username = username;
    }

    public void setPassword(String password){
        format.password = password;
    }

    public void setDatabase(String database){
        format.database = database;
    }

    public void setCollection(String collection){
        format.collectionName = collection;
    }

    public void setColumns(List<MetaColumn> columns){
        format.columns = columns;
    }

    public void setMode(String mode){
        format.mode = mode;
    }

    public void setReplaceKey(String replaceKey){
        format.replaceKey = replaceKey;
    }


    public void setMongodbConfig(Map<String,Object> mongodbConfig){
        format.mongodbConfig = mongodbConfig;
    }

    @Override
    protected void checkFormat() {
        if(format.hostPorts == null){
            throw new IllegalArgumentException("No host supplied");
        }

        if(format.database == null){
            throw new IllegalArgumentException("No database supplied");
        }

        if(format.collectionName == null){
            throw new IllegalArgumentException("No collection supplied");
        }
    }
}
