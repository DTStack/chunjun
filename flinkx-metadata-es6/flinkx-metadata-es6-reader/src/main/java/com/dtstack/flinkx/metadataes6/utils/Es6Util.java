package com.dtstack.flinkx.metadataes6.utils;

import com.dtstack.flinkx.metadataes6.constants.MetaDataEs6Cons;
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
import java.util.*;

public class Es6Util {
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

    public static Map<String, Object> queryIndexProp(String indexName,RestClient restClient) throws IOException {
        Map<String, Object> indexProp = new HashMap<>(16);
        String method = "GET";
        String endpoint1 = "/_cat/indices";
        String endpoint2 = "/"+indexName;
        Map<String, String> params = Collections.singletonMap("index", indexName);
        Response response1 = restClient.performRequest(method,endpoint1,params);
        Response response2 = restClient.performRequest(method,endpoint2);
        String resBody1 = EntityUtils.toString(response1.getEntity());
        String resBody2 = EntityUtils.toString(response2.getEntity());
        Map map = GsonUtil.GSON.fromJson(resBody2, GsonUtil.gsonMapTypeToken);
        String [] arr = resBody1.split("\\s+");
        int n = 0;
        indexProp.put("1",arr[n++]);
        indexProp.put("2",arr[n++]);
        indexProp.put("3",arr[n++]);
        indexProp.put("4",arr[n++]);
        indexProp.put("5",arr[n++]);
        indexProp.put("6",arr[n++]);
        indexProp.put("7",arr[n++]);
        indexProp.put("8",arr[n++]);
        indexProp.put("9",arr[n++]);
        indexProp.put("10",arr[n]);
        indexProp.put("11",map);
        return indexProp;
    }
}
