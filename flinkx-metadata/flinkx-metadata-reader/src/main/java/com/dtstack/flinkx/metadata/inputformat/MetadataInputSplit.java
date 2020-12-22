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

import com.google.gson.GsonBuilder;
import org.apache.flink.core.io.InputSplit;

import java.util.List;

/**
 * @author : tiezhu
 * @date : 2020/3/20
 */
public class MetadataInputSplit implements InputSplit {

    private static final long serialVersionUID = -4483633039887822171L;

    private int splitNumber;

    private String dbName;

    private List<Object> tableList;

    public MetadataInputSplit(int splitNumber, String dbName, List<Object> tableList) {
        this.splitNumber = splitNumber;
        this.dbName = dbName;
        this.tableList = tableList;
    }

    public String getDbName() {
        return dbName;
    }

    public List<Object> getTableList() {
        return tableList;
    }

    @Override
    public String toString() {
        return "MetadataInputSplit{" +
                "splitNumber=" + splitNumber +
                ", dbName='" + dbName + '\'' +
                ", tableList=" + new GsonBuilder().serializeNulls().create().toJson(tableList) +
                '}';
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }
}

