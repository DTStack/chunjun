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

package com.dtstack.chunjun.connector.doris.rest;

import com.dtstack.chunjun.connector.doris.exception.DorisConnectFailedException;
import com.dtstack.chunjun.connector.doris.options.DorisConf;
import com.dtstack.chunjun.connector.doris.options.LoadConf;
import com.dtstack.chunjun.connector.doris.rest.module.Backend;
import com.dtstack.chunjun.connector.doris.rest.module.BackendRow;
import com.dtstack.chunjun.connector.doris.rest.module.PartitionDefinition;
import com.dtstack.chunjun.connector.doris.rest.module.QueryPlan;
import com.dtstack.chunjun.connector.doris.rest.module.Schema;
import com.dtstack.chunjun.connector.doris.rest.module.Tablet;

import org.apache.flink.shaded.guava18.com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.doris.options.DorisKeys.CONNECT_FAILED_MESSAGE;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_REQUEST_RETRIES_DEFAULT;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_TABLET_SIZE;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_TABLET_SIZE_DEFAULT;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_TABLET_SIZE_MIN;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.ILLEGAL_ARGUMENT_MESSAGE;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.SHOULD_NOT_HAPPEN_MESSAGE;

/**
 * @author tiezhu@dtstack
 * @date 2021/9/17 星期五
 */
public class FeRestService implements Serializable {

    public static final int REST_RESPONSE_STATUS_OK = 200;
    public static final int REST_RESPONSE_CODE_OK = 0;
    private static final Logger LOG = LoggerFactory.getLogger(FeRestService.class);
    private static final String REST_RESPONSE_BE_ROWS_KEY = "rows";
    private static final String API_PREFIX = "/api";
    private static final String SCHEMA = "_schema";
    private static final String QUERY_PLAN = "_query_plan";
    private static final String BACKENDS = "/rest/v1/system?path=//backends";
    private static final String FE_LOGIN = "/rest/v1/login";

    /**
     * send request to Doris FE and get response json string.
     *
     * @param options configuration of request
     * @param request {@link HttpRequestBase} real request
     * @return Doris FE response in json string
     * @throws DorisConnectFailedException throw when cannot connect to Doris FE
     */
    private static String send(DorisConf options, HttpRequestBase request)
            throws DorisConnectFailedException {
        LoadConf loadConf = options.getLoadConf();
        int connectTimeout =
                loadConf.getRequestConnectTimeoutMs() == null
                        ? DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT
                        : loadConf.getRequestConnectTimeoutMs();
        int socketTimeout =
                loadConf.getRequestReadTimeoutMs() == null
                        ? DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT
                        : loadConf.getRequestReadTimeoutMs();
        int retries =
                loadConf.getRequestRetries() == null
                        ? DORIS_REQUEST_RETRIES_DEFAULT
                        : loadConf.getRequestRetries();
        LOG.trace(
                "connect timeout set to '{}'. socket timeout set to '{}'. retries set to '{}'.",
                connectTimeout,
                socketTimeout,
                retries);

        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(connectTimeout)
                        .setSocketTimeout(socketTimeout)
                        .build();

        request.setConfig(requestConfig);
        LOG.info(
                "Send request to Doris FE '{}' with user '{}'.",
                request.getURI(),
                options.getUsername());
        IOException ex = null;
        int statusCode = -1;

        for (int attempt = 0; attempt < retries; attempt++) {
            LOG.debug("Attempt {} to request {}.", attempt, request.getURI());
            try {
                String response;
                if (request instanceof HttpGet) {
                    response =
                            get(
                                    request.getURI().toString(),
                                    options.getUsername(),
                                    options.getPassword());
                } else {
                    response =
                            getConnectionPost(
                                    request, options.getUsername(), options.getPassword());
                }
                LOG.warn(
                        "Failed to get response from Doris FE {}, http code is {}",
                        request.getURI(),
                        statusCode);
                LOG.debug(
                        String.format(
                                "Success get response from Doris FE: %s, response is: %s.",
                                request.getURI(), response));
                // Handle the problem of inconsistent data format returned by http v1 and v2
                ObjectMapper mapper = new ObjectMapper();
                Map map = mapper.readValue(response, Map.class);
                if (map.containsKey("code") && map.containsKey("msg")) {
                    Object data = map.get("data");
                    return mapper.writeValueAsString(data);
                } else {
                    return response;
                }
            } catch (IOException e) {
                ex = e;
                LOG.warn(CONNECT_FAILED_MESSAGE, request.getURI(), e);
            }
        }

        LOG.error(CONNECT_FAILED_MESSAGE, request.getURI(), ex);
        throw new DorisConnectFailedException(
                options.getUsername(), request.getURI().toString(), ex);
    }

    private static String getConnectionPost(HttpRequestBase request, String user, String passwd)
            throws IOException {
        URL url = new URL(request.getURI().toString());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod(request.getMethod());
        String authEncoding =
                Base64.getEncoder()
                        .encodeToString(
                                String.format("%s:%s", user, passwd)
                                        .getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", "Basic " + authEncoding);
        InputStream content = ((HttpPost) request).getEntity().getContent();
        String res = IOUtils.toString(content);
        conn.setDoOutput(true);
        conn.setDoInput(true);
        PrintWriter out = new PrintWriter(conn.getOutputStream());
        // send request params
        out.print(res);
        // flush
        out.flush();
        // read response
        return parseResponse(conn);
    }

    private static String get(String request, String user, String passwd) throws IOException {
        URL realUrl = new URL(request);
        // open connection
        HttpURLConnection connection = (HttpURLConnection) realUrl.openConnection();
        String authEncoding =
                Base64.getEncoder()
                        .encodeToString(
                                String.format("%s:%s", user, passwd)
                                        .getBytes(StandardCharsets.UTF_8));
        connection.setRequestProperty("Authorization", "Basic " + authEncoding);

        connection.connect();
        return parseResponse(connection);
    }

    private static String parseResponse(HttpURLConnection connection) throws IOException {
        if (connection.getResponseCode() != HttpStatus.SC_OK) {
            LOG.warn(
                    "Failed to get response from Doris  {}, http code is {}",
                    connection.getURL(),
                    connection.getResponseCode());
            throw new IOException("Failed to get response from Doris");
        }
        StringBuilder result = new StringBuilder();
        BufferedReader in =
                new BufferedReader(
                        new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
        String line;
        while ((line = in.readLine()) != null) {
            result.append(line);
        }
        in.close();
        return result.toString();
    }

    /**
     * parse table identifier to array.
     *
     * @param tableIdentifier table identifier string
     * @return first element is db name, second element is table name
     * @throws IllegalArgumentException table identifier is illegal
     */
    @VisibleForTesting
    static String[] parseIdentifier(String tableIdentifier) throws IllegalArgumentException {
        LOG.trace("Parse identifier '{}'.", tableIdentifier);
        if (StringUtils.isEmpty(tableIdentifier)) {
            LOG.error(ILLEGAL_ARGUMENT_MESSAGE, "table.identifier", tableIdentifier);
            throw new IllegalArgumentException("table.identifier: " + tableIdentifier);
        }
        String[] identifier = tableIdentifier.split("\\.");
        if (identifier.length != 2) {
            LOG.error(ILLEGAL_ARGUMENT_MESSAGE, "table.identifier", tableIdentifier);
            throw new IllegalArgumentException("table.identifier: " + tableIdentifier);
        }
        return identifier;
    }

    /**
     * choice a Doris FE node to request.
     *
     * @param feNodes Doris FE node list, separate be comma
     * @return the chosen one Doris FE node
     * @throws IllegalArgumentException fe nodes is illegal
     */
    @VisibleForTesting
    static String randomEndpoint(List<String> feNodes) throws IllegalArgumentException {
        LOG.trace("Parse feNodes '{}'.", feNodes);
        if (feNodes.isEmpty()) {
            LOG.error(ILLEGAL_ARGUMENT_MESSAGE, "feNodes", feNodes);
            throw new IllegalArgumentException("feNodes: " + feNodes);
        }
        Collections.shuffle(feNodes);
        return feNodes.get(0).trim();
    }

    /**
     * choice a Doris BE node to request.
     *
     * @param options configuration of request
     * @return the chosen one Doris BE node
     * @throws IllegalArgumentException BE nodes is illegal
     */
    @VisibleForTesting
    public static String randomBackend(DorisConf options) throws IOException {
        List<BackendRow> backends = getBackends(options);
        LOG.trace("Parse beNodes '{}'.", backends);
        if (backends == null || backends.isEmpty()) {
            LOG.error(ILLEGAL_ARGUMENT_MESSAGE, "beNodes", backends);
            throw new IllegalArgumentException("beNodes: " + backends);
        }
        Collections.shuffle(backends);
        BackendRow backend = backends.get(0);
        return backend.getIP() + ":" + backend.getHttpPort();
    }

    /**
     * get Doris BE nodes to request.
     *
     * @param options configuration of request
     * @return the chosen one Doris BE node
     * @throws IllegalArgumentException BE nodes is illegal
     */
    @VisibleForTesting
    static List<BackendRow> getBackends(DorisConf options) throws IOException {
        List<String> feNodes = options.getFeNodes();
        String feNode = randomEndpoint(feNodes);
        String beUrl = "http://" + feNode + BACKENDS;
        HttpGet httpGet = new HttpGet(beUrl);
        String response = send(options, httpGet);
        LOG.info("Backend Info:{}", response);
        return parseBackend(response);
    }

    static List<BackendRow> parseBackend(String response) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Backend backend;
        try {
            backend = mapper.readValue(response, Backend.class);
        } catch (JsonParseException e) {
            String errMsg = "Doris BE's response is not a json. res: " + response;
            LOG.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "Doris BE's response cannot map to schema. res: " + response;
            LOG.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse Doris BE's response to json failed. res: " + response;
            LOG.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }

        if (backend == null) {
            LOG.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new RuntimeException("This exception is unsupported. Check the code.");
        }
        List<BackendRow> backendRows =
                backend.getRows().stream()
                        .filter(BackendRow::getAlive)
                        .collect(Collectors.toList());
        LOG.debug("Parsing schema result is '{}'.", backendRows);
        return backendRows;
    }

    /**
     * get a valid URI to connect Doris FE.
     *
     * @param options configuration of request
     * @return uri string
     * @throws IllegalArgumentException throw when configuration is illegal
     */
    @VisibleForTesting
    static String getUriStr(DorisConf options) throws IllegalArgumentException {
        return "http://"
                + randomEndpoint(options.getFeNodes())
                + API_PREFIX
                + "/"
                + options.getDatabase()
                + "/"
                + options.getTable()
                + "/";
    }

    /**
     * discover Doris table schema from Doris FE.
     *
     * @param options configuration of request
     * @return Doris table schema
     * @throws RuntimeException throw when discover failed
     */
    public static Schema getSchema(DorisConf options) throws RuntimeException {
        LOG.trace("Finding schema.");
        HttpGet httpGet = new HttpGet(getUriStr(options) + SCHEMA);
        String response = send(options, httpGet);
        LOG.debug("Find schema response is '{}'.", response);
        return parseSchema(response);
    }

    /**
     * translate Doris FE response to inner {@link Schema} struct.
     *
     * @param response Doris FE response
     * @return inner {@link Schema} struct
     * @throws RuntimeException throw when translate failed
     */
    @VisibleForTesting
    public static Schema parseSchema(String response) throws RuntimeException {
        LOG.trace("Parse response '{}' to schema.", response);
        ObjectMapper mapper = new ObjectMapper();
        Schema schema;
        try {
            schema = mapper.readValue(response, Schema.class);
        } catch (JsonParseException e) {
            String errMsg = "Doris FE's response is not a json. res: " + response;
            LOG.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "Doris FE's response cannot map to schema. res: " + response;
            LOG.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse Doris FE's response to json failed. res: " + response;
            LOG.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }

        if (schema == null) {
            LOG.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new RuntimeException("This exception is unsupported. Check the code.");
        }

        if (schema.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "Doris FE's response is not OK, status is " + schema.getStatus();
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }
        LOG.debug(String.format("Parsing schema result is '%s'.", schema));
        return schema;
    }

    /**
     * find Doris RDD partitions from Doris FE.
     *
     * @param options configuration of request
     * @return an list of Doris RDD partitions
     * @throws RuntimeException throw when find partition failed
     */
    public static List<PartitionDefinition> findPartitions(DorisConf options)
            throws RuntimeException {
        LoadConf loadConf = options.getLoadConf();
        String readFields =
                StringUtils.isBlank(loadConf.getReadFields()) ? "*" : loadConf.getReadFields();
        String sql =
                "select "
                        + readFields
                        + " from `"
                        + options.getTable()
                        + "`.`"
                        + options.getDatabase()
                        + "`";
        if (!StringUtils.isEmpty(loadConf.getFilterQuery())) {
            sql += " where " + loadConf.getFilterQuery();
        }
        LOG.debug("Query SQL Sending to Doris FE is: '{}'.", sql);

        HttpPost httpPost = new HttpPost(getUriStr(options) + QUERY_PLAN);
        String entity = "{\"sql\": \"" + sql + "\"}";
        LOG.debug("Post body Sending to Doris FE is: '{}'.", entity);
        StringEntity stringEntity = new StringEntity(entity, StandardCharsets.UTF_8);
        stringEntity.setContentEncoding("UTF-8");
        stringEntity.setContentType("application/json");
        httpPost.setEntity(stringEntity);

        String resStr = send(options, httpPost);
        LOG.debug("Find partition response is '{}'.", resStr);
        QueryPlan queryPlan = getQueryPlan(resStr);
        Map<String, List<Long>> be2Tablets = selectBeForTablet(queryPlan);
        return tabletsMapToPartition(
                options,
                be2Tablets,
                queryPlan.getOpaqued_query_plan(),
                options.getTable(),
                options.getDatabase());
    }

    /**
     * translate Doris FE response string to inner {@link QueryPlan} struct.
     *
     * @param response Doris FE response string
     * @return inner {@link QueryPlan} struct
     * @throws RuntimeException throw when translate failed.
     */
    @VisibleForTesting
    static QueryPlan getQueryPlan(String response) throws RuntimeException {
        ObjectMapper mapper = new ObjectMapper();
        QueryPlan queryPlan;
        try {
            queryPlan = mapper.readValue(response, QueryPlan.class);
        } catch (JsonParseException e) {
            String errMsg = "Doris FE's response is not a json. res: " + response;
            LOG.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "Doris FE's response cannot map to schema. res: " + response;
            LOG.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse Doris FE's response to json failed. res: " + response;
            LOG.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }

        if (queryPlan == null) {
            LOG.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new RuntimeException("This exception is unsupported. Check the code.");
        }

        if (queryPlan.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "Doris FE's response is not OK, status is " + queryPlan.getStatus();
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }
        LOG.debug("Parsing partition result is '{}'.", queryPlan);
        return queryPlan;
    }

    /**
     * select which Doris BE to get tablet data.
     *
     * @param queryPlan {@link QueryPlan} translated from Doris FE response
     * @return BE to tablets {@link Map}
     * @throws RuntimeException throw when select failed.
     */
    @VisibleForTesting
    static Map<String, List<Long>> selectBeForTablet(QueryPlan queryPlan) throws RuntimeException {
        Map<String, List<Long>> be2Tablets = new HashMap<>();
        for (Map.Entry<String, Tablet> part : queryPlan.getPartitions().entrySet()) {
            LOG.debug("Parse tablet info: '{}'.", part);
            long tabletId;
            try {
                tabletId = Long.parseLong(part.getKey());
            } catch (NumberFormatException e) {
                String errMsg = "Parse tablet id '" + part.getKey() + "' to long failed.";
                LOG.error(errMsg, e);
                throw new RuntimeException(errMsg, e);
            }
            String target = null;
            int tabletCount = Integer.MAX_VALUE;
            for (String candidate : part.getValue().getRouting()) {
                LOG.trace("Evaluate Doris BE '{}' to tablet '{}'.", candidate, tabletId);
                if (!be2Tablets.containsKey(candidate)) {
                    LOG.debug("Choice a new Doris BE '{}' for tablet '{}'.", candidate, tabletId);
                    List<Long> tablets = new ArrayList<>();
                    be2Tablets.put(candidate, tablets);
                    target = candidate;
                    break;
                } else {
                    if (be2Tablets.get(candidate).size() < tabletCount) {
                        target = candidate;
                        tabletCount = be2Tablets.get(candidate).size();
                        LOG.debug(
                                "Current candidate Doris BE to tablet '{}' is '{}' with tablet count {}.",
                                tabletId,
                                target,
                                tabletCount);
                    }
                }
            }
            if (target == null) {
                String errMsg = "Cannot choice Doris BE for tablet " + tabletId;
                LOG.error(errMsg);
                throw new RuntimeException(errMsg);
            }

            LOG.debug("Choice Doris BE '{}' for tablet '{}'.", target, tabletId);
            be2Tablets.get(target).add(tabletId);
        }
        return be2Tablets;
    }

    /**
     * tablet count limit for one Doris RDD partition
     *
     * @param loadConf configuration of request
     * @return tablet count limit
     */
    @VisibleForTesting
    static int tabletCountLimitForOnePartition(LoadConf loadConf) {
        int tabletsSize = DORIS_TABLET_SIZE_DEFAULT;
        if (loadConf.getRequestTabletSize() != null) {
            tabletsSize = loadConf.getRequestTabletSize();
        }
        if (tabletsSize < DORIS_TABLET_SIZE_MIN) {
            LOG.warn(
                    "{} is less than {}, set to default value {}.",
                    DORIS_TABLET_SIZE,
                    DORIS_TABLET_SIZE_MIN,
                    DORIS_TABLET_SIZE_MIN);
            tabletsSize = DORIS_TABLET_SIZE_MIN;
        }
        LOG.debug("Tablet size is set to {}.", tabletsSize);
        return tabletsSize;
    }

    /**
     * translate BE tablets map to Doris RDD partition.
     *
     * @param options configuration of request
     * @param be2Tablets BE to tablets {@link Map}
     * @param opaquedQueryPlan Doris BE execute plan getting from Doris FE
     * @param database database name of Doris table
     * @param table table name of Doris table
     * @return Doris RDD partition {@link List}
     * @throws IllegalArgumentException throw when translate failed
     */
    @VisibleForTesting
    static List<PartitionDefinition> tabletsMapToPartition(
            DorisConf options,
            Map<String, List<Long>> be2Tablets,
            String opaquedQueryPlan,
            String database,
            String table)
            throws IllegalArgumentException {
        int tabletsSize = tabletCountLimitForOnePartition(options.getLoadConf());
        List<PartitionDefinition> partitions = new ArrayList<>();
        for (Map.Entry<String, List<Long>> beInfo : be2Tablets.entrySet()) {
            LOG.debug("Generate partition with beInfo: '{}'.", beInfo);
            HashSet<Long> tabletSet = new HashSet<>(beInfo.getValue());
            beInfo.getValue().clear();
            beInfo.getValue().addAll(tabletSet);
            int first = 0;
            while (first < beInfo.getValue().size()) {
                Set<Long> partitionTablets =
                        new HashSet<>(
                                beInfo.getValue()
                                        .subList(
                                                first,
                                                Math.min(
                                                        beInfo.getValue().size(),
                                                        first + tabletsSize)));
                first = first + tabletsSize;
                PartitionDefinition partitionDefinition =
                        new PartitionDefinition(
                                database,
                                table,
                                options,
                                beInfo.getKey(),
                                partitionTablets,
                                opaquedQueryPlan);
                LOG.debug("Generate one PartitionDefinition '{}'.", partitionDefinition);
                partitions.add(partitionDefinition);
            }
        }
        return partitions;
    }
}
