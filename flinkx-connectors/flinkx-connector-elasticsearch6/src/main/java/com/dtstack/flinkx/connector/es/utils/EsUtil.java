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

package com.dtstack.flinkx.connector.es.utils;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;

import com.dtstack.flinkx.connector.es.conf.EsConf;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.dtstack.flinkx.connector.es.utils.EsConstants.ES_DEFAULT_PORT;
import static com.dtstack.flinkx.connector.es.utils.EsConstants.ES_DEFAULT_SCHEMA;
import static com.dtstack.flinkx.connector.es.utils.EsConstants.SEPARATOR;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.HOSTS_OPTION;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/19 12:41
 */
public class EsUtil {

    /**
     *
     * @param elasticsearchConf
     * @return
     */
    public static RestHighLevelClient createClient(EsConf elasticsearchConf) {
        List<HttpHost> httpAddresses = getHosts(elasticsearchConf.getHosts());
        RestClientBuilder restClientBuilder = RestClient.builder(
                httpAddresses.
                        toArray(new HttpHost[httpAddresses.size()]));
        if (elasticsearchConf.isAuthMesh()) {
            // basic auth
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(elasticsearchConf.getUsername(),
                            elasticsearchConf.getPassword()));
            restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        RestHighLevelClient rhlClient = new RestHighLevelClient(restClientBuilder);
        return rhlClient;
    }


    /**
     * parse address to HttpHosts
     */
    public static List<HttpHost> parseAddress(String address){
        Preconditions.checkArgument(address != null, "address is not allowed null.");
        List<String> addressStrs = Arrays.asList(address.split(SEPARATOR));

        Preconditions.checkArgument(addressStrs.size() != 0, "address is not null.");
        return addressStrs.stream()
                .map(add -> add.split(":"))
                .map(addressArray -> {
                    String host = addressArray[0].trim();
                    int port = addressArray.length > 1 ? Integer.valueOf(addressArray[1].trim()) : ES_DEFAULT_PORT;
                    return new HttpHost(host.trim(), port, ES_DEFAULT_SCHEMA);
                }).collect(Collectors.toList());
    }

    /**
     * generate doc id by id fields.
     * @param
     * @return
     */
    public static String generateDocId(List<String> idFieldNames, Map<String, Object> dataMap, String keyDelimiter) {
        String doc_id = "";
        if (null != idFieldNames) {
            doc_id = idFieldNames.stream()
                    .map(idFiledName -> dataMap.get(idFiledName).toString())
                    .collect(Collectors.joining(keyDelimiter));
        }
        return doc_id;
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
