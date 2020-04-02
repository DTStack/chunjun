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

package com.dtstack.flinkx.cassandra.reader;

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

/**
 * The builder for cassandra reader plugin
 *
 * @Company: www.dtstack.com
 * @author wuhui
 */
public class CassandraInputFormatBuilder extends BaseRichInputFormatBuilder {

    private CassandraInputFormat format;

    public CassandraInputFormatBuilder() {
        super.format = format = new CassandraInputFormat();
    }

    public void setTable(String table){
        format.table = table;
    }

    public void setWhere(String where) {format.whereString = where;}

    public void setConsistancyLevel(String consistancyLevel) {format.consistancyLevel = consistancyLevel;}

    public void setAllowFiltering(boolean allowFiltering) {format.allowFiltering = allowFiltering;}

    public void setKeySpace(String keySpace) {format.keySpace = keySpace;}

    public void setColumn(List<MetaColumn> column) {format.columnMeta = column;}

    public void setCassandraConfig(Map<String,Object> cassandraConfig){
        format.cassandraConfig = cassandraConfig;
    }

    @Override
    protected void checkFormat() {
        Preconditions.checkNotNull(format.table, "table must not null");

        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
    }
}
