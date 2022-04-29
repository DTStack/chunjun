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

package com.dtstack.chunjun.connector.elasticsearch5.utils;

import com.dtstack.chunjun.connector.elasticsearch5.conf.ElasticsearchConf;

import org.apache.flink.streaming.connectors.elasticsearch.util.ElasticsearchUtils;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.IOUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.Netty3Plugin;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.elasticsearch5.utils.ElasticsearchConstants.ES_DEFAULT_PORT;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.HOSTS_OPTION;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/27 23:54
 */
public class ElasticsearchUtil {

    private static Logger LOG = LoggerFactory.getLogger(ElasticsearchUtil.class);

    public static TransportClient createClient(ElasticsearchConf elasticsearchConf) {

        Map<String, Object> mapSetting = new HashMap<>();
        mapSetting.put("cluster.name", elasticsearchConf.getCluster());

        List<InetSocketAddress> transports = new ArrayList<>();
        for (String address : elasticsearchConf.getHosts()) {
            String[] infoArray = StringUtils.split(address, ":");
            String host = infoArray[0];
            int port = ES_DEFAULT_PORT;
            if (infoArray.length > 1) {
                port = Integer.valueOf(infoArray[1].trim());
            }
            try {
                transports.add(new InetSocketAddress(InetAddress.getByName(host), port));
            } catch (UnknownHostException e) {
                LOG.error("", e);
                throw new RuntimeException(e);
            }
        }

        boolean authMesh = elasticsearchConf.isAuthMesh();
        if (authMesh) {
            String authPassword =
                    elasticsearchConf.getUsername() + ":" + elasticsearchConf.getPassword();
            mapSetting.put("xpack.security.user", authPassword);
        }

        Settings settings =
                Settings.builder()
                        .put(mapSetting)
                        //
                        // https://stackoverflow.com/questions/43585724/elasticsearch-transport-client-caused-by-java-lang-nosuchmethoderror-io-netty
                        .put(NetworkModule.HTTP_TYPE_KEY, Netty3Plugin.NETTY_HTTP_TRANSPORT_NAME)
                        .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty3Plugin.NETTY_TRANSPORT_NAME)
                        .build();

        TransportClient transportClient;
        if (elasticsearchConf.isAuthMesh()) {
            transportClient = new PreBuiltXPackTransportClient(settings);
        } else {
            transportClient = new PreBuiltTransportClient(settings);
        }

        for (TransportAddress transport :
                ElasticsearchUtils.convertInetSocketAddresses(transports)) {
            transportClient.addTransportAddress(transport);
        }

        // verify that we actually are connected to a cluster
        if (transportClient.connectedNodes().isEmpty()) {
            // close the transportClient here
            IOUtils.closeQuietly(transportClient);
            throw new RuntimeException(
                    "Elasticsearch client is not connected to any Elasticsearch nodes!");
        }

        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Created Elasticsearch TransportClient with connected nodes {}",
                    transportClient.connectedNodes());
        }

        return transportClient;
    }

    /**
     * parse address to InetSocketAddress
     *
     * @param esAddressList
     * @return
     */
    public static List<InetSocketAddress> parseAddress(List<String> esAddressList) {
        List<InetSocketAddress> transports = new ArrayList<>();

        for (String address : esAddressList) {
            String[] infoArray = StringUtils.split(address, ":");
            int port = 9300;
            String host = infoArray[0];
            if (infoArray.length > 1) {
                port = Integer.valueOf(infoArray[1].trim());
            }

            try {
                transports.add(new InetSocketAddress(InetAddress.getByName(host), port));
            } catch (Exception e) {
                LOG.error("", e);
                throw new RuntimeException(e);
            }
        }
        return transports;
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
