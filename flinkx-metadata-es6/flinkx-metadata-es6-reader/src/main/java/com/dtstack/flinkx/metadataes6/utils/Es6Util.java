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
package com.dtstack.flinkx.metadataes6.utils;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.metadataes6.constants.MetaDataEs6Cons;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.TelnetUtil;
import com.dtstack.flinkx.metadataes6.entity.AliasEntity;
import com.dtstack.flinkx.metadataes6.entity.ColumnEntity;
import com.dtstack.flinkx.metadataes6.entity.FieldEntity;
import com.dtstack.flinkx.metadataes6.entity.IndexProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author : baiyu
 * @date : 2020/12/3
 */
public class Es6Util {

    /**
     * 建立LowLevelRestClient连接
     * @param url   es服务端地址，"ip:port"
     * @param username  用户名
     * @param password  密码
     * @return  LowLevelRestClient
     */
    public static RestClient getClient(String url, String username, String password) {
        List<HttpHost> httpHostList = new ArrayList<>();
        String[] addr = url.split(ConstantValue.COMMA_SYMBOL);
        for(String add : addr) {
            String[] pair = add.split(ConstantValue.COLON_SYMBOL);
            TelnetUtil.telnet(pair[0], Integer.parseInt(pair[1]));
            httpHostList.add(new HttpHost(pair[0], Integer.parseInt(pair[1]), ConstantValue.KEY_HTTP));
        }

        RestClientBuilder builder = RestClient.builder(httpHostList.toArray(new HttpHost[0]));

        if(StringUtils.isNotBlank(username)){
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder.disableAuthCaching();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            });
        }

        return builder.build();
    }

    /**
     * 返回指定索引的配置信息
     * @param indexName 索引名称
     * @param restClient    ES6 LowLevelRestClient
     * @return  索引的配置信息
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static IndexProperties queryIndexProp(String indexName, RestClient restClient) throws IOException {

        IndexProperties indexProperties = new IndexProperties();

        /*
         * 字符串数组indexByCat的数据结构如下，数字下标对于具体变量位置
         * 0      1      2        3                      4   5   6          7            8          9
         * health status index    uuid                   pri rep docs.count docs.deleted store.size pri.store.size
         * yellow open   megacorp LYXJZVslTaiTOtQzVFqfLg   5   1          0            0      1.2kb          1.2kb
         */
        String [] indexByCat = queryIndexByCat(indexName, restClient);

        indexProperties.setHealth(indexByCat[0]);
        indexProperties.setStatus(indexByCat[1]);
        indexProperties.setUuid(indexByCat[3]);
        indexProperties.setDocsCount(indexByCat[6]);
        indexProperties.setDocsDeleted(indexByCat[7]);
        indexProperties.setTotalSize(indexByCat[8]);
        indexProperties.setPriSize( indexByCat[9]);

        Map<String, Object> index = queryIndex(indexName, restClient);
        Map<String, Object> settings = getMap(getMap(index, indexName), MetaDataEs6Cons.MAP_SETTINGS);
        Map<String, String> indexSetting = ( Map<String, String>) settings.get(MetaDataEs6Cons.KEY_INDEX);

        indexProperties.setCreateTime(indexSetting.get(MetaDataEs6Cons.MAP_CREATION_DATE));
        indexProperties.setShards(indexSetting.get(MetaDataEs6Cons.MAP_NUMBER_OF_SHARDS));
        indexProperties.setReplicas(indexSetting.get(MetaDataEs6Cons.MAP_NUMBER_OF_REPLICAS));

        return indexProperties;

    }

    /**
     * 查询指定索引下的所有字段信息
     * @param indexName 索引名称
     * @param restClient    ES6 LowLevelRestClient
     * @return  字段信息
     * @throws IOException
     */
    public static List<ColumnEntity> queryColumns(String indexName, RestClient restClient) throws IOException {

        Map<String, Object> index = queryIndex(indexName, restClient);
        Map<String, Object> mappings = getMap(getMap(index, indexName), MetaDataEs6Cons.MAP_MAPPINGS);

        if (mappings.isEmpty()){
            return new ArrayList<>();
        }

        //这里level表示map需向下读取两次
        int level = 2;
        for (int i = 0; i < level; i++) {
            List<String> keys = new ArrayList<>(mappings.keySet());
            mappings = getMap(mappings, keys.get(0));
        }

        return getColumn(mappings, new StringBuilder(), new ArrayList<>(16));

    }

    /**
     * 返回字段列表
     * @param docs  未经处理包含所有字段信息的map
     * @param columnName    字段名
     * @param columnList    处理后的字段列表
     * @return  字段列表
     */
    public static List<ColumnEntity> getColumn(Map<String, Object> docs, StringBuilder columnName, List<ColumnEntity> columnList){
        for(String key : docs.keySet()){
            if (MetaDataEs6Cons.MAP_PROPERTIES.equals(key)){
                getColumn(getMap(docs, key), columnName, columnList);
                break;
            }else if(MetaDataEs6Cons.MAP_TYPE.equals(key)){
                ColumnEntity column = new ColumnEntity();
                String columnTempName = columnName.toString();
                column.setColumnName(columnTempName);
                column.setDateType(docs.get(key).toString());
                if (docs.get(MetaDataEs6Cons.KEY_FIELDS) != null){
                    column.setFieldList(getFieldList(docs));
                }
                int columnIndex = columnList.size() + 1;
                column.setColumnIndex(columnIndex);
                columnList.add(column);
                break;
            } else {
                String temp = columnName.toString();
                if (temp.length() == 0){
                    columnName.append(key);
                }else {
                    columnName.append(ConstantValue.POINT_SYMBOL).append(key);
                }
                getColumn(getMap(docs, key), columnName, columnList);
                columnName = new StringBuilder(temp);
            }
        }

        return columnList;
    }

    /**
     * 返回字段映射参数
     * @param docs  该字段属性map
     * @return 字段映射参数
     */
    @SuppressWarnings("unchecked")
    public static List<FieldEntity> getFieldList(Map<String, Object> docs) {
        Map<String, Map<String, String>> fields = (Map<String, Map<String, String>>) docs.get(MetaDataEs6Cons.KEY_FIELDS);
        List<FieldEntity> fieldsList = new ArrayList<>(16);
        FieldEntity field = new FieldEntity();
        for (String key: fields.keySet()) {
            field.setFieldName(key);
            field.setFieldProp(fields.get(key).toString());
            fieldsList.add(field);
        }

        return fieldsList;
    }

    /**
     * 查询索引别名
     * @param indexName 索引名称
     * @param restClient     ES6 LowLevelRestClient
     * @return 索引别名
     * @throws IOException
     */
    public static List<AliasEntity> queryAliases(String indexName, RestClient restClient) throws IOException {

        List<AliasEntity> aliasList = new ArrayList<>();
        AliasEntity alias = new AliasEntity();
        Map<String, Object> index = queryIndex(indexName, restClient);
        Map<String, Object> aliases = getMap(getMap(index, indexName), MetaDataEs6Cons.MAP_ALIASES);

        for (String key: aliases.keySet()) {
            alias.setAliasName(key);
            alias.setAliasProp(aliases.get(key).toString());
            aliasList.add(alias);
        }

        return aliasList;
    }

    /**
     * 使用/_cat/indices{index}的方式查询指定index
     * @param restClient     ES6 LowLevelRestClient
     * @param indexName     索引名称
     * @return index
     * @throws IOException
     */
    public static String[] queryIndexByCat(String indexName, RestClient restClient) throws IOException {

        Map<String, String> params = Collections.singletonMap(MetaDataEs6Cons.KEY_INDEX, indexName);
        Response response = restClient.performRequest(MetaDataEs6Cons.API_METHOD_GET, MetaDataEs6Cons.API_ENDPOINT_CAT_INDEX, params);
        String resBody = EntityUtils.toString(response.getEntity());

        return  resBody.split("\\s+");
    }

    /**
     * indexName为*表示查询所有的索引信息
     * @param restClient     ES6 LowLevelRestClient
     * @return
     * @throws IOException
     */
    public static String[] queryIndicesByCat(RestClient restClient) throws IOException {
        return queryIndexByCat(ConstantValue.STAR_SYMBOL, restClient);
    }

    /**
     * 使用/index的方式查询指定索引的详细信息
     * @param indexName     索引名称
     * @param restClient     ES6 LowLevelRestClient
     * @return
     * @throws IOException
     */
    public static Map<String, Object> queryIndex(String indexName, RestClient restClient) throws IOException {

        String endpoint = ConstantValue.SINGLE_SLASH_SYMBOL + indexName;
        Response response = restClient.performRequest(MetaDataEs6Cons.API_METHOD_GET, endpoint);
        String resBody = EntityUtils.toString(response.getEntity());

        return GsonUtil.GSON.fromJson(resBody, GsonUtil.gsonMapTypeToken);

    }

    /**
     *  将map中key对应的value值转换成Map<String, Object>并返回
     * @param map
     * @param key
     * @return 强转后的value值
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> getMap(Map<String, Object> map, String key){
        try {
            return (Map<String, Object>) map.get(key);
        }catch (Exception e){
            throw new RuntimeException("Map类型强转失败.", e);
        }
    }
}
