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

package com.dtstack.chunjun.connector.hbase.util;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static com.dtstack.chunjun.connector.hbase.util.HBaseConfigUtils.KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL;

public class HBaseConfigUtilsTest {

    private static final String KEY_HBASE_SECURITY_AUTHORIZATION = "hbase.security.authorization";

    public static final String KRB_STR = "Kerberos";

    public static final String KEY_PRINCIPAL = "hbase.principal";

    @Test
    public void testGetConfig() {
        final String k1 = "where?";
        final String v1 = "I'm on a boat";
        final String k2 = "when?";
        final String v2 = "midnight";
        final String k3 = "why?";
        final String v3 = "what do you think?";
        final String k4 = "which way?";
        final String v4 = "south, always south...";

        Map<String, Object> confMap = Maps.newHashMap();
        confMap.put(k1, v1);
        confMap.put(k2, v2);
        confMap.put(k3, v3);
        confMap.put(k4, v4);

        Configuration config = HBaseConfigUtils.getConfig(confMap);

        Assert.assertEquals(v1, config.get(k1));
    }

    @Test
    public void testIsEnableKerberos() {
        Map<String, Object> confMap = Maps.newHashMap();

        confMap.put(KEY_HBASE_SECURITY_AUTHORIZATION, KRB_STR);

        Configuration config = HBaseConfigUtils.getConfig(confMap);

        Assert.assertTrue(HBaseConfigUtils.isEnableKerberos(confMap));
        Assert.assertTrue(HBaseConfigUtils.isEnableKerberos(config));
    }

    @Test
    public void testGetPrincipal() {
        Map<String, Object> confMap = Maps.newHashMap();
        String principal = "test_principal";

        confMap.put(KEY_PRINCIPAL, principal);

        Assert.assertEquals(principal, HBaseConfigUtils.getPrincipal(confMap));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPrincipalWithEmptyPrincipal() {
        Map<String, Object> confMap = Maps.newHashMap();

        HBaseConfigUtils.getPrincipal(confMap);
    }

    @Test
    public void testFillKerberosConfig() {
        Map<String, Object> confMap = Maps.newHashMap();
        String principal = "test_principal";

        confMap.put(KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL, principal);
        confMap.put(KEY_PRINCIPAL, principal);
        HBaseConfigUtils.fillKerberosConfig(confMap);

        Assert.assertEquals(KRB_STR, confMap.get(KEY_HBASE_SECURITY_AUTHORIZATION));
    }
}
