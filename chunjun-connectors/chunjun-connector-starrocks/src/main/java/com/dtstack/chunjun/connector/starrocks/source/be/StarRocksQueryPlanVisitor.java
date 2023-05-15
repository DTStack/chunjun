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

package com.dtstack.chunjun.connector.starrocks.source.be;

import com.dtstack.chunjun.connector.starrocks.config.StarRocksConfig;
import com.dtstack.chunjun.connector.starrocks.source.be.entity.QueryBeXTablets;
import com.dtstack.chunjun.connector.starrocks.source.be.entity.QueryInfo;
import com.dtstack.chunjun.connector.starrocks.source.be.entity.QueryPlan;
import com.dtstack.chunjun.connector.starrocks.source.be.entity.Tablet;
import com.dtstack.chunjun.util.ThreadUtil;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.starrocks.util.StarRocksUtil.getBasicAuthHeader;

@Slf4j
public class StarRocksQueryPlanVisitor implements Serializable {

    private static final long serialVersionUID = -1509905823010569772L;

    private final StarRocksConfig starRocksConfig;

    public StarRocksQueryPlanVisitor(StarRocksConfig starRocksConfig) {
        this.starRocksConfig = starRocksConfig;
    }

    public QueryInfo getQueryInfo(String querySql) throws IOException {
        List<String> httpNodeList = starRocksConfig.getFeNodes();
        QueryPlan queryPlan =
                getQueryPlan(querySql, httpNodeList.get(new Random().nextInt(httpNodeList.size())));
        return new QueryInfo(queryPlan, transferQueryPlanToBeXTablet(queryPlan));
    }

    private static List<QueryBeXTablets> transferQueryPlanToBeXTablet(QueryPlan queryPlan) {
        Map<String, Set<Long>> beXTablets = new HashMap<>();
        queryPlan
                .getPartitions()
                .forEach(
                        (tabletId, tablet) -> allocateTabletsEqually(beXTablets, tabletId, tablet));

        return beXTablets.entrySet().stream()
                .map(
                        entry ->
                                new QueryBeXTablets(
                                        entry.getKey(), new ArrayList<>(entry.getValue())))
                .collect(Collectors.toList());
    }

    private static void allocateTabletsEqually(
            Map<String, Set<Long>> beXTablets, String tabletId, Tablet tablet) {
        int tabletCount = Integer.MAX_VALUE;
        String currentBeNode = "";
        // Allocate tablets equally for all BeNodes
        for (String beNode : tablet.getRoutings()) {
            if (!beXTablets.containsKey(beNode)) {
                beXTablets.put(beNode, new HashSet<>());
                currentBeNode = beNode;
                break;
            }
            if (beXTablets.get(beNode).size() < tabletCount) {
                currentBeNode = beNode;
                tabletCount = beXTablets.get(beNode).size();
            }
        }
        beXTablets.get(currentBeNode).add(Long.valueOf(tabletId));
    }

    public QueryPlan getQueryPlan(String querySql, String httpNode) throws IOException {
        String url = getRequestUrl(httpNode);
        Map<String, Object> bodyMap = new HashMap<>();
        bodyMap.put("sql", querySql);
        String body = new JSONObject(bodyMap).toString();

        int requestCode = 0;
        String respString = "";
        for (int i = 0; i < starRocksConfig.getMaxRetries(); i++) {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpPost post = new HttpPost(url);
                post.setHeader("Content-Type", "application/json;charset=UTF-8");
                post.setHeader(
                        "Authorization",
                        getBasicAuthHeader(
                                starRocksConfig.getUsername(), starRocksConfig.getPassword()));
                post.setEntity(new ByteArrayEntity(body.getBytes()));
                try (CloseableHttpResponse response = httpClient.execute(post)) {
                    requestCode = response.getStatusLine().getStatusCode();
                    HttpEntity respEntity = response.getEntity();
                    respString = EntityUtils.toString(respEntity, "UTF-8");
                }
            }
            if (200 == requestCode || i == starRocksConfig.getMaxRetries()) {
                break;
            }
            log.warn("Request of get query plan failed with code:{}", requestCode);
            ThreadUtil.sleepMilliseconds(1000L * (i + 1));
        }
        return dealQueryPlanResult(requestCode, respString);
    }

    private QueryPlan dealQueryPlanResult(int requestCode, String respString) {
        if (200 != requestCode) {
            throw new RuntimeException(
                    "Request of get query plan failed with code " + requestCode + " " + respString);
        }
        if (respString.isEmpty()) {
            log.warn("Request failed with empty response.");
            throw new RuntimeException("Request failed with empty response." + requestCode);
        }
        JSONObject jsonObject = JSONObject.parseObject(respString);
        return JSONObject.toJavaObject(jsonObject, QueryPlan.class);
    }

    private String getRequestUrl(String httpNode) {
        return "http://"
                + httpNode
                + "/api/"
                + starRocksConfig.getDatabase()
                + "/"
                + starRocksConfig.getTable()
                + "/_query_plan";
    }
}
