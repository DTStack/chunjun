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

package com.dtstack.flinkx.connector.elasticsearch7;

import com.dtstack.flinkx.security.SSLUtil;
import com.dtstack.flinkx.util.MapUtil;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.table.api.ValidationException;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.HOSTS_OPTION;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/27 17:32
 */
public class Elasticsearch7ClientFactory {

    /**
     * @param elasticsearchConf
     * @return
     */
    public static RestHighLevelClient createClient(
            ElasticsearchConf elasticsearchConf, DistributedCache distributedCache) {
        boolean useSsl = Objects.nonNull(elasticsearchConf.getSslConfig());
        List<HttpHost> httpAddresses = getHosts(elasticsearchConf.getHosts(), useSsl);
        RestClientBuilder restClientBuilder =
                RestClient.builder(httpAddresses.toArray(new HttpHost[httpAddresses.size()]));
        restClientBuilder.setRequestConfigCallback(
                requestConfigBuilder ->
                        requestConfigBuilder
                                .setConnectTimeout(elasticsearchConf.getConnectTimeout())
                                .setConnectionRequestTimeout(elasticsearchConf.getRequestTimeout())
                                .setSocketTimeout(elasticsearchConf.getSocketTimeout()));
        // 进行用户和密码认证
        if (elasticsearchConf.getPassword() != null && elasticsearchConf.getUsername() != null) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(
                            elasticsearchConf.getUsername(), elasticsearchConf.getPassword()));

            restClientBuilder.setHttpClientConfigCallback(
                    httpAsyncClientBuilder -> {
                        HttpAsyncClientBuilder httpAsyncClientBuilder1 =
                                httpAsyncClientBuilder
                                        .setDefaultCredentialsProvider(credentialsProvider)
                                        .setKeepAliveStrategy(
                                                (response, context) ->
                                                        elasticsearchConf.getKeepAliveTime())
                                        .setMaxConnPerRoute(elasticsearchConf.getMaxConnPerRoute());
                        if (useSsl) {
                            httpAsyncClientBuilder1.setSSLContext(
                                    getSslContext(
                                            elasticsearchConf.getSslConfig(), distributedCache));
                            httpAsyncClientBuilder1.setSSLHostnameVerifier(
                                    NoopHostnameVerifier.INSTANCE);
                        }
                        return httpAsyncClientBuilder1;
                    });
        } else {
            restClientBuilder.setHttpClientConfigCallback(
                    httpAsyncClientBuilder ->
                            httpAsyncClientBuilder
                                    .setKeepAliveStrategy(
                                            (response, context) ->
                                                    elasticsearchConf.getKeepAliveTime())
                                    .setMaxConnPerRoute(elasticsearchConf.getMaxConnPerRoute()));
        }
        return new RestHighLevelClient(restClientBuilder);
    }

    private static List<HttpHost> getHosts(List<String> hosts, boolean ssl) {
        if (ssl) {
            hosts =
                    hosts.stream()
                            .map(
                                    i -> {
                                        if (!i.startsWith("https://")) {
                                            return "https://" + i;
                                        }
                                        return i;
                                    })
                            .collect(Collectors.toList());
        }
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

    private static SSLContext getSslContext(SslConf sslConf, DistributedCache distributedCache) {
        try {
            String path = sslConf.getFileName();
            if (StringUtils.isNotBlank(sslConf.getFilePath())) {
                path = sslConf.getFilePath() + File.separator + sslConf.getFileName();
            }
            String file = SSLUtil.loadFile(MapUtil.objectToMap(sslConf), path, distributedCache);
            Path trustStorePath = Paths.get(file);
            // use the certificate to obtain KeyStore
            KeyStore trustStore =
                    SSLUtil.getKeyStoreByType(
                            sslConf.getType(), trustStorePath, sslConf.getKeyStorePass());

            SSLContextBuilder sslBuilder =
                    SSLContexts.custom()
                            .loadTrustMaterial(trustStore, (x509Certificates, s) -> true);
            return sslBuilder.build();

        } catch (Exception e) {
            throw new RuntimeException("get sslContext failed ", e);
        }
    }
}
