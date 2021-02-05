/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.metadata.inputformat;

import com.dtstack.flinkx.util.GsonUtil;
import org.apache.flink.core.io.InputSplit;

import java.util.List;

/** 元数据查询的子任务
 * @author kunni@dtstack.com
 */
public class MetadataBaseInputSplit  implements InputSplit {

    private static final long serialVersionUID = -4483633039887822171L;

    /**分片编号*/
    private int splitNumber;

    /**分片对应的库名，分片规则为一个库对应一个分片*/
    protected String dbName;

    /**为了兼容查询table或者schema table结构*/
    protected List<Object> tableList;

    public MetadataBaseInputSplit(int splitNumber, String dbName, List<Object> tableList) {
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
                ", tableList=" + GsonUtil.GSON.toJson(tableList) +
                '}';
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }
}
