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

package com.dtstack.flinkx.es.reader;

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import java.util.List;
import java.util.Map;

/**
 * The builder class of EsInputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class EsInputFormatBuilder extends BaseRichInputFormatBuilder {

    private EsInputFormat format;

    public EsInputFormatBuilder() {
        super.format = format = new EsInputFormat();
    }

    public EsInputFormatBuilder setAddress(String address) {
        format.address = address;
        return this;
    }

    public EsInputFormatBuilder setUsername(String username) {
        format.username = username;
        return this;
    }

    public EsInputFormatBuilder setPassword(String password) {
        format.password = password;
        return this;
    }

    public EsInputFormatBuilder setQuery(String query) {
        format.query = query;
        return this;
    }

    public EsInputFormatBuilder setColumnNames(String query) {
        format.query = query;
        return this;
    }

    public EsInputFormatBuilder setColumnNames(List<String> columnNames) {
        format.columnNames = columnNames;
        return this;
    }

    public EsInputFormatBuilder setColumnValues(List<String> columnValues) {
        format.columnValues = columnValues;
        return this;
    }

    public EsInputFormatBuilder setColumnTypes(List<String> columnTypes) {
        format.columnTypes = columnTypes;
        return this;
    }

    public EsInputFormatBuilder setIndex(String[] index){
        format.index = index;
        return this;
    }

    public EsInputFormatBuilder setType(String[] type){
        format.type = type;
        return this;
    }

    public EsInputFormatBuilder setBatchSize(Integer batchSize){
        if(batchSize != null && batchSize > 0){
            format.batchSize = batchSize;
        }
        return this;
    }

    public EsInputFormatBuilder setClientConfig(Map<String, Object> clientConfig){
        format.clientConfig = clientConfig;
        return this;
    }

    @Override
    protected void checkFormat() {
        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
    }
}
