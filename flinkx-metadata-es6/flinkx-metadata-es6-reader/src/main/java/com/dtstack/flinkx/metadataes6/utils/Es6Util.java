package com.dtstack.flinkx.metadataes6.utils;

import com.dtstack.flinkx.metadataes6.constants.MetaDataEs6Cons;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.TelnetUtil;
import org.apache.commons.collections.MapUtils;
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
import java.text.SimpleDateFormat;
import java.util.*;

public class Es6Util {

    /**
     * 建立LowLevelRestClient连接
     * @param address   es服务端地址，"ip:port"
     * @param username  用户名
     * @param password  密码
     * @param config    配置
     * @return  LowLevelRestClient
     */
    public static RestClient getClient(String address, String username, String password, Map<String,Object> config) {
        List<HttpHost> httpHostList = new ArrayList<>();
        String[] addr = address.split(",");
        for(String add : addr) {
            String[] pair = add.split(":");
            TelnetUtil.telnet(pair[0], Integer.parseInt(pair[1]));
            httpHostList.add(new HttpHost(pair[0], Integer.parseInt(pair[1]), "http"));
        }

        RestClientBuilder builder = RestClient.builder(httpHostList.toArray(new HttpHost[0]));

        Integer timeout = MapUtils.getInteger(config, MetaDataEs6Cons.KEY_TIMEOUT);
        if (timeout != null){
            builder.setMaxRetryTimeoutMillis(timeout * 1000);
        }

        String pathPrefix = MapUtils.getString(config, MetaDataEs6Cons.KEY_PATH_PREFIX);
        if (StringUtils.isNotEmpty(pathPrefix)){
            builder.setPathPrefix(pathPrefix);
        }
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
    public static Map<String, Object> queryIndexProp(String indexName,RestClient restClient) throws IOException {
        Map<String, Object> indexProp = new HashMap<>(16);

        String [] prop_1 = queryIndexByCat(indexName,restClient);
        indexProp.put(MetaDataEs6Cons.KEY_INDEX_UUID,prop_1[3]);
        indexProp.put(MetaDataEs6Cons.KEY_INDEX_SIZE,prop_1[8]);
        indexProp.put(MetaDataEs6Cons.KEY_INDEX_DOCS_COUNT,prop_1[6]);

        Map<String,Object> index = queryIndex(indexName,restClient);
        Map<String,Object> settings = ( Map<String,Object>) (( Map<String,Object>) index.get(indexName)).get("settings");
        settings = ( Map<String,Object>) settings.get(MetaDataEs6Cons.KEY_INDEX);
        Object creation_date = formatDate(settings.get("creation_date"));
        Object shards = settings.get("number_of_shards");
        Object replicas = settings.get("number_of_replicas");
        indexProp.put(MetaDataEs6Cons.KEY_INDEX_CREATE_TIME,creation_date);
        indexProp.put(MetaDataEs6Cons.KEY_INDEX_SHARDS,shards);
        indexProp.put(MetaDataEs6Cons.KEY_INDEX_REP,replicas);

        return indexProp;
    }

    /**
     * 查询指定索引下的所有字段信息
     * @param indexName 索引名称
     * @param restClient    ES6 LowLevelRestClient
     * @return  字段信息
     * @throws IOException
     */
    public static List<Map<String, Object>> queryColumns(String indexName,RestClient restClient) throws IOException {

        List<Map<String, Object>> columnList = new ArrayList<>();
        Map<String,Object> index = queryIndex(indexName,restClient);
        Map<String,Object> mappings = (Map<String, Object>) ((Map<String,Object>) index.get(indexName)).get("mappings");

        if (mappings.isEmpty()){
            return columnList;
        }

        for (int i = 0; i < 2; i++) {
            List<String> keys = new ArrayList(mappings.keySet());
            mappings = (Map<String, Object>) mappings.get(keys.get(0));
        }

        return getColumn(mappings,"",new ArrayList<>());
    }

    /**
     * 返回字段列表
     * @param docs  未经处理包含所有字段信息的map
     * @param columnName    字段名
     * @param columnList    处理后的字段列表
     * @return  字段列表
     */
    public static List<Map<String, Object>> getColumn(Map<String,Object> docs,String columnName,List<Map<String, Object>> columnList){

        for(String key : docs.keySet()){
            if (key.equals("properties")){
                getColumn((Map<String, Object>) docs.get(key),columnName,columnList);
                break;
            }else if(key.equals("type")){
                Map<String,Object> column = new HashMap<>();
                column.put(MetaDataEs6Cons.KEY_COLUMN_NAME,columnName);
                column.put(MetaDataEs6Cons.KEY_DATA_TYPE,docs.get(key));
                if (docs.get(MetaDataEs6Cons.KEY_FIELDS) != null){
                    column.put(MetaDataEs6Cons.KEY_FIELDS,getFieldList(docs));
                }
                columnList.add(column);
                break;
            } else {
                String temp = columnName;
                if (columnName.equals("")){
                    columnName = key;
                }else {
                    columnName = columnName + "." + key;
                }
                getColumn((Map<String, Object>) docs.get(key),columnName,columnList);
                columnName = temp;
            }
        }

        return columnList;
    }

    /**
     * 返回字段映射参数
     * @param docs  该字段属性map
     * @return
     */
    public static List<Map<String, Object>> getFieldList(Map<String,Object> docs){
        Map<String,Object> fields = (Map<String, Object>) docs.get("fields");
        Iterator<Map.Entry<String,Object>> it = fields.entrySet().iterator();
        List<Map<String,Object>> fieldsList = new ArrayList<>();
        Map<String,Object> field = new HashMap();
        while (it.hasNext()){
            Map.Entry<String,Object> entry = it.next();
            field.put(MetaDataEs6Cons.KEY_FIELD_NAME,entry.getKey());
            field.put(MetaDataEs6Cons.KEY_FIELD_PROP,entry.getValue());
            fieldsList.add(field);
        }

        return fieldsList;
    }

    /**
     * 查询索引别名
     * @param indexName 索引名称
     * @param restClient     ES6 LowLevelRestClient
     * @return
     * @throws IOException
     */
    public static List<Map<String, Object>> queryAliases(String indexName,RestClient restClient) throws IOException {
        List<Map<String, Object>> aliasList = new ArrayList<>();
        Map<String,Object> alias = new HashMap();
        Map<String,Object> index = queryIndex(indexName,restClient);
        Map<String,Object> aliases = (Map<String, Object>) ((Map<String,Object>) index.get(indexName)).get("aliases");
        Iterator<Map.Entry<String,Object>> it = aliases.entrySet().iterator();

        while (it.hasNext()){
            Map.Entry<String,Object> entry = it.next();
            alias.put("aliase_name",entry.getKey());
            alias.put("aliase_prop",entry.getValue());
            aliasList.add(alias);
        }
        return aliasList;
    }

    /**
     * 使用/_cat/indices{index}的方式查询指定index
     * @param restClient     ES6 LowLevelRestClient
     * @param indexName     索引名称
     * @return
     * @throws IOException
     */
    public static String[] queryIndexByCat(String indexName,RestClient restClient) throws IOException {
        String endpoint = "/_cat/indices";
        Map<String, String> params = Collections.singletonMap(MetaDataEs6Cons.KEY_INDEX, indexName);
        Response response = restClient.performRequest(MetaDataEs6Cons.API_METHOD_GET,endpoint,params);
        String resBody = EntityUtils.toString(response.getEntity());
        String [] indices = resBody.split("\\s+");
        return indices;
    }

    /**
     * indexName为*表示查询所有的索引信息
     * @param restClient     ES6 LowLevelRestClient
     * @return
     * @throws IOException
     */
    public static String[] queryIndicesByCat(RestClient restClient) throws IOException {
        return queryIndexByCat("*",restClient);
    }

    /**
     * 使用/index的方式查询指定索引的详细信息
     * @param indexName     索引名称
     * @param restClient     ES6 LowLevelRestClient
     * @return
     * @throws IOException
     */
    public static Map<String,Object> queryIndex(String indexName,RestClient restClient) throws IOException {
        String endpoint = "/"+indexName;
        Response response = restClient.performRequest(MetaDataEs6Cons.API_METHOD_GET,endpoint);
        String resBody = EntityUtils.toString(response.getEntity());
        Map<String,Object> index = GsonUtil.GSON.fromJson(resBody, GsonUtil.gsonMapTypeToken);
        return index;
    }

    /**
     * 格式化日期
     * @param date
     * @return
     */
    public static Object formatDate (Object date){
        long long_time =Long.parseLong(date.toString());
        Date date_time = new Date(long_time);
        SimpleDateFormat format = DateUtil.getDateTimeFormatter();
        date = format.format(date_time);
        return date;
    }
}
