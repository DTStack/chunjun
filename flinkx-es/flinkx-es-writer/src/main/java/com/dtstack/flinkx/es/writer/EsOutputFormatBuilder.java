/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.es.writer;

import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import java.util.List;
import java.util.Map;

/**
 * The Builder class of EsOutputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class EsOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private EsOutputFormat format;

    public EsOutputFormatBuilder() {
        super.format = format = new EsOutputFormat();
    }

    public void setAddress(String address) {
        format.address = address;
    }


    public void setUsername(String username) {
        format.username = username;
    }

    public void setPassword(String password) {
        format.password = password;
    }

    public void setIdColumnIndices(List<Integer> idColumnIndices) {
        format.idColumnIndices = idColumnIndices;
    }

    public void setIdColumnTypes(List<String> idColumnTypes) {
        format.idColumnTypes = idColumnTypes;
    }

    public void setIdColumnValues(List<String> idColumnValues) {
        format.idColumnValues = idColumnValues;
    }

    public void setType(String type) {
        format.type = type;
    }

    public void setIndex(String index) {
        format.index = index;
    }

    public void setColumnNames(List<String> columnNames) {
        format.columnNames = columnNames;
    }

    public void setColumnTypes(List<String> columnTypes) {
        format.columnTypes = columnTypes;
    }

    public EsOutputFormatBuilder setClientConfig(Map<String, Object> clientConfig){
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
