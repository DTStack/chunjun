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
package com.dtstack.flinkx.metadata.inputformat;

import com.dtstack.flinkx.metadata.MetaDataCons;
import com.google.gson.Gson;
import org.apache.flink.core.io.InputSplit;

import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/20
 */
public class MetadataDbTableList {

    private static final long serialVersionUID = -4483633039887822171L;

    protected List<Map<String, Object>> dbTableList;

    protected int position = 0;

    public MetadataDbTableList(List<Map<String, Object>> dbTableList) {
        this.dbTableList = dbTableList;
    }

    public String getDbName() {
        return (String)dbTableList.get(position).get(MetaDataCons.KEY_DB_NAME);
    }

    public List<String> getTableList() {
        return (List<String>) dbTableList.get(position).get(MetaDataCons.KEY_TABLE_LIST);
    }

    public void increPosition(){
        position++;
    }

    public boolean reachEndPosition(){
        return position==dbTableList.size();
    }


    @Override
    public String toString() {
        return "tableList=" + new Gson().toJson(dbTableList) + ", position=" + position;
    }

}
