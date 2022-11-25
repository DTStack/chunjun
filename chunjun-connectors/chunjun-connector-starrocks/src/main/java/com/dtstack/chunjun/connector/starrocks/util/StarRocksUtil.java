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

package com.dtstack.chunjun.connector.starrocks.util;

import com.dtstack.chunjun.connector.starrocks.source.be.entity.QueryBeXTablets;
import com.dtstack.chunjun.connector.starrocks.source.be.entity.QueryInfo;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.commons.codec.binary.Base64;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StarRocksUtil {

    public static String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }

    public static List<List<QueryBeXTablets>> splitQueryBeXTablets(
            int subTaskCount, QueryInfo queryInfo) {
        List<List<QueryBeXTablets>> curBeXTabletList = new ArrayList<>();
        for (int i = 0; i < subTaskCount; i++) {
            curBeXTabletList.add(new ArrayList<>());
        }
        int beXTabletsListCount = queryInfo.getBeXTablets().size();
        /* subTaskCount <= beXTabletsListCount */
        if (subTaskCount <= beXTabletsListCount) {
            for (int i = 0; i < beXTabletsListCount; i++) {
                curBeXTabletList.get(i % subTaskCount).add(queryInfo.getBeXTablets().get(i));
            }
            return curBeXTabletList;
        }
        /* subTaskCount > beXTabletsListCount */
        // split to singleTabletList
        List<Tuple2<String, Long>> beXTabletTupleList = new ArrayList<>();
        for (QueryBeXTablets queryBeXTablets : queryInfo.getBeXTablets()) {
            for (Long tabletId : queryBeXTablets.getTabletIds()) {
                beXTabletTupleList.add(new Tuple2<>(queryBeXTablets.getBeNode(), tabletId));
            }
        }
        int stepLen = beXTabletTupleList.size() / subTaskCount;
        // beWithSingleTabletList.size()<=subTaskCount
        if (stepLen <= 1) {
            for (int i = 0; i < beXTabletTupleList.size(); i++) {
                Tuple2<String, Long> beXTabletTuple = beXTabletTupleList.get(i);
                curBeXTabletList.set(
                        i,
                        Collections.singletonList(
                                new QueryBeXTablets(
                                        beXTabletTuple.f0,
                                        Collections.singletonList(beXTabletTuple.f1))));
            }
            return curBeXTabletList;
        }
        // beWithSingleTabletList.size()>subTaskCount
        int remain = beXTabletTupleList.size() % subTaskCount;
        int end = 0;
        for (int i = 0; i < subTaskCount; i++) {
            int start = end;
            end += stepLen;
            if (i < remain) {
                end += 1;
            }
            List<Tuple2<String, Long>> curBeTabletTupleList =
                    beXTabletTupleList.subList(start, end);
            Map<String, List<Long>> beXTabletsMap = new HashMap<>();
            curBeTabletTupleList.forEach(
                    curBeTabletTuple -> {
                        List<Long> curTabletList =
                                beXTabletsMap.getOrDefault(curBeTabletTuple.f0, new ArrayList<>());
                        curTabletList.add(curBeTabletTuple.f1);
                        beXTabletsMap.put(curBeTabletTuple.f0, curTabletList);
                    });

            curBeXTabletList.set(
                    i,
                    beXTabletsMap.entrySet().stream()
                            .map(entry -> new QueryBeXTablets(entry.getKey(), entry.getValue()))
                            .collect(Collectors.toList()));
        }
        return curBeXTabletList;
    }

    public static String addStrForNum(String str, int strLength, String appendStr) {
        int strLen = str.length();
        if (strLen < strLength) {
            StringBuilder strBuilder = new StringBuilder(str);
            while (strLen < strLength) {
                strBuilder.append(appendStr);
                strLen = strBuilder.length();
            }
            str = strBuilder.toString();
        }
        return str;
    }
}
