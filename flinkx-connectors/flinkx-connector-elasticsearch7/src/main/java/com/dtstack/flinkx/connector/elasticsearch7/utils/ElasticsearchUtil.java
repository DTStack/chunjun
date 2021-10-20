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

package com.dtstack.flinkx.connector.elasticsearch7.utils;

import com.dtstack.flinkx.connector.elasticsearch7.conf.ElasticsearchConf;

import org.apache.flink.table.api.ValidationException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.HOSTS_OPTION;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/27 17:32
 */
public class ElasticsearchUtil {

    /**
     * @param elasticsearchConf
     * @return
     */
    public static RestHighLevelClient createClient(ElasticsearchConf elasticsearchConf) {
        List<HttpHost> httpAddresses = getHosts(elasticsearchConf.getHosts());
        RestClientBuilder restClientBuilder =
                RestClient.builder(httpAddresses.toArray(new HttpHost[httpAddresses.size()]));
        restClientBuilder.setRequestConfigCallback(
                requestConfigBuilder ->
                        requestConfigBuilder
                                .setConnectTimeout(elasticsearchConf.getConnectTimeout())
                                .setConnectionRequestTimeout(elasticsearchConf.getRequestTimeout())
                                .setSocketTimeout(elasticsearchConf.getSocketTimeout()));
        if (elasticsearchConf.isAuthMesh()) {
            // 进行用户和密码认证
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(
                            elasticsearchConf.getUsername(), elasticsearchConf.getPassword()));
            restClientBuilder.setHttpClientConfigCallback(
                    httpAsyncClientBuilder ->
                            httpAsyncClientBuilder
                                    .setDefaultCredentialsProvider(credentialsProvider)
                                    .setKeepAliveStrategy(
                                            (response, context) ->
                                                    elasticsearchConf.getKeepAliveTime())
                                    .setMaxConnPerRoute(elasticsearchConf.getMaxConnPerRoute()));

        } else {
            restClientBuilder.setHttpClientConfigCallback(
                    httpAsyncClientBuilder ->
                            httpAsyncClientBuilder
                                    .setKeepAliveStrategy(
                                            (response, context) ->
                                                    elasticsearchConf.getKeepAliveTime())
                                    .setMaxConnPerRoute(elasticsearchConf.getMaxConnPerRoute()));
        }

        RestHighLevelClient rhlClient = new RestHighLevelClient(restClientBuilder);
        return rhlClient;
    }

    /**
     * generate doc id by id fields.
     *
     * @param
     * @return
     */
    public static String generateDocId(
            List<String> idFieldNames, Map<String, Object> dataMap, String keyDelimiter) {
        String docId = "";
        if (null != idFieldNames) {
            docId =
                    idFieldNames.stream()
                            .map(idFiledName -> dataMap.get(idFiledName).toString())
                            .collect(Collectors.joining(keyDelimiter));
        }
        return docId;
    }

    public static List<HttpHost> getHosts(List<String> hosts) {
        return hosts.stream()
                .map(host -> validateAndParseHostsString(host))
                .collect(Collectors.toList());
    }

    /**
     * Parse Hosts String to list.
     *
     * <p>Hosts String format was given as following:
     *
     * <pre>
     *     connector.hosts = http://host_name:9092;http://host_name:9093
     * </pre>
     */
    private static HttpHost validateAndParseHostsString(String host) {
        try {
            HttpHost httpHost = HttpHost.create(host);
            if (httpHost.getPort() < 0) {
                throw new ValidationException(
                        String.format(
                                "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'. Missing port.",
                                host, HOSTS_OPTION.key()));
            }

            if (httpHost.getSchemeName() == null) {
                throw new ValidationException(
                        String.format(
                                "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'. Missing scheme.",
                                host, HOSTS_OPTION.key()));
            }
            return httpHost;
        } catch (Exception e) {
            throw new ValidationException(
                    String.format(
                            "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'.",
                            host, HOSTS_OPTION.key()),
                    e);
        }
    }
}
