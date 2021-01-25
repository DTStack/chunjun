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
package com.dtstack.flinkx.metadataes6.format;

import com.dtstack.flinkx.metadata.core.BaseCons;
import com.dtstack.flinkx.metadata.entity.MetadataEntity;
import com.dtstack.flinkx.metadata.inputformat.MetadataBaseInputFormat;
import com.dtstack.flinkx.metadata.inputformat.MetadataBaseInputSplit;
import com.dtstack.flinkx.metadataes6.utils.Es6Util;
import com.dtstack.flinkx.metadataes6.entity.AliasEntity;
import com.dtstack.flinkx.metadataes6.entity.ColumnEntity;
import com.dtstack.flinkx.metadataes6.entity.IndexProperties;
import com.dtstack.flinkx.metadataes6.entity.MetaDataEs6Entity;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.core.io.InputSplit;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author : baiyu
 * @date : 2020/12/3
 */
public class Metadataes6InputFormat extends MetadataBaseInputFormat {

    protected String url;

    protected String username;

    protected String password;

    protected String currentIndex;

    private transient RestClient restClient;

    @Override
    protected void doOpenInternal() throws IOException {
        restClient = Es6Util.getClient(url, username, password);
        if(CollectionUtils.isEmpty(tableList)){
            tableList = showIndices();
        }

        LOG.debug("indicesSize = {}, indices = {}", tableList.size(), tableList);
    }

    /**
     * 此处分片数为默认值1
     * @param splitNumber
     * @return
     */
    @Override
    protected InputSplit[] createInputSplitsInternal(int splitNumber) {
        InputSplit[] inputSplits = new MetadataBaseInputSplit[originalJob.size()];
        for (int index = 0; index < originalJob.size(); index++) {
            Map<String, Object> dbTables = originalJob.get(index);
            String dbName = MapUtils.getString(dbTables, BaseCons.KEY_DB_NAME);
            List<Object> tables = (List<Object>) dbTables.get(BaseCons.KEY_TABLE_LIST);
            inputSplits[index] = new MetadataBaseInputSplit(splitNumber, dbName, tables);
        }
        return inputSplits;
    }

    @Override
    public MetadataEntity createMetadataEntity() throws IOException {
        currentIndex = (String) currentObject;
        IndexProperties indexProperties = Es6Util.queryIndexProp(currentIndex, restClient);
        List<AliasEntity> aliasList = Es6Util.queryAliases(currentIndex, restClient);
        List<ColumnEntity> columnList = Es6Util.queryColumns(currentIndex, restClient);
        MetaDataEs6Entity metaDataEs6Entity = new MetaDataEs6Entity();
        metaDataEs6Entity.setIndexName(currentIndex);
        metaDataEs6Entity.setColumn(columnList);
        metaDataEs6Entity.setIndexProperties(indexProperties);
        metaDataEs6Entity.setAlias(aliasList);

        return metaDataEs6Entity;
    }

    @Override
    protected void closeInternal() throws IOException {
        if(restClient != null) {
            restClient.close();
            restClient = null;
        }
    }

    /**
     * 返回es集群下的所有索引
     * @return  索引列表
     * @throws IOException
     */
    protected List<Object> showIndices() throws IOException {

        List<Object> indexNameList = new ArrayList<>();
        String[] indexArr = Es6Util.queryIndicesByCat(restClient);
        /*
         * 字符串数组indexArr的数据结构如下，数组下标positon对应具体变量位置,此处需循环读取所有的index，即下标为position+differrence*n的数组的值
         * 0      1      2        3                      4   5   6          7            8          9
         * health status index    uuid                   pri rep docs.count docs.deleted store.size pri.store.size
         * yellow open   megacorp LYXJZVslTaiTOtQzVFqfLg   5   1          0            0      1.2kb          1.2kb
         * yellow open   test     oixXJg2jThG82H_Y4ZpgcA   5   1          0            0      1.2kb          1.2kb
         */
        int position = 2, difference = 10;
        while (position < indexArr.length) {
            indexNameList.add(indexArr[position]);
            position += difference;
        }

        return indexNameList;
    }

    public void setUsername(String username){
        this.username = username;
    }

    public void setPassword(String password){
        this.password = password;
    }

    public void setUrl(String url){
        this.url = url;
    }
}
