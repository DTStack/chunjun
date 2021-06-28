package com.dtstack.flinkx.connector.elasticsearch5.utils;

import com.dtstack.flinkx.connector.elasticsearch5.conf.ElasticsearchConf;

import org.apache.commons.lang3.StringUtils;

import org.apache.flink.streaming.connectors.elasticsearch.util.ElasticsearchUtils;
import org.apache.flink.table.api.ValidationException;

import com.google.common.base.Preconditions;

import org.apache.flink.util.IOUtils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.dtstack.flinkx.connector.elasticsearch5.utils.ElasticsearchConstants.ES_DEFAULT_PORT;
import static com.dtstack.flinkx.connector.elasticsearch5.utils.ElasticsearchConstants.ES_DEFAULT_SCHEMA;
import static com.dtstack.flinkx.connector.elasticsearch5.utils.ElasticsearchConstants.SEPARATOR;
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
        Settings settings = Settings.builder().put(elasticsearchConf)
                .build();

        TransportClient transportClient;
        if (elasticsearchConf.isAuthMesh()) {
            transportClient = new PreBuiltXPackTransportClient(settings);
        }else {
            transportClient = new PreBuiltTransportClient(settings);
        }
        List<InetSocketAddress> hosts = parseAddress(elasticsearchConf.getHosts());
        for (TransportAddress transport : ElasticsearchUtils.convertInetSocketAddresses(hosts)) {
            transportClient.addTransportAddress(transport);
        }

        // verify that we actually are connected to a cluster
        if (transportClient.connectedNodes().isEmpty()) {

            // close the transportClient here
            IOUtils.closeQuietly(transportClient);

            throw new RuntimeException("Elasticsearch client is not connected to any Elasticsearch nodes!");
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Created Elasticsearch TransportClient with connected nodes {}", transportClient.connectedNodes());
        }

        return transportClient;
    }

    /**
     * parse address to InetSocketAddress
     * @param esAddressList
     * @return
     */
    public static List<InetSocketAddress> parseAddress(List<String> esAddressList) {
        List<InetSocketAddress> transports = new ArrayList<>();

        for(String address : esAddressList){
            String[] infoArray = StringUtils.split(address, ":");
            int port = 9300;
            String host = infoArray[0];
            if(infoArray.length > 1){
                port = Integer.valueOf(infoArray[1].trim());
            }

            try {
                transports.add(new InetSocketAddress(InetAddress.getByName(host), port));
            }catch (Exception e){
                LOG.error("", e);
                throw new RuntimeException(e);
            }
        }
        return transports;
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
