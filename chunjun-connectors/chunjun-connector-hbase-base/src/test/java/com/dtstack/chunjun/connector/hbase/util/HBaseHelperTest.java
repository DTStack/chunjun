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

import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;
import com.dtstack.chunjun.security.KerberosUtil;
import com.dtstack.chunjun.util.FileSystemUtil;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Map;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    HBaseHelperTest.class,
    ConnectionFactory.class,
    HBaseConfiguration.class,
    KerberosUtil.class,
    FileSystemUtil.class
})
@PowerMockIgnore("javax.management.*")
public class HBaseHelperTest {

    private static final String KEY_HBASE_SECURITY_AUTHORIZATION = "hbase.security.authorization";
    public static final String KRB_STR = "Kerberos";

    private Connection mockConnection;

    @Before
    public void setUp() throws IOException {

        Configuration mockConfig = mock(Configuration.class);
        this.mockConnection = mock(Connection.class);

        PowerMockito.mockStatic(ConnectionFactory.class);
        PowerMockito.mockStatic(HBaseConfiguration.class);
        PowerMockito.mockStatic(KerberosUtil.class);
        PowerMockito.mockStatic(FileSystemUtil.class);

        when(HBaseConfiguration.create()).thenAnswer((Answer<Configuration>) answer -> mockConfig);
        when(ConnectionFactory.createConnection(mockConfig))
                .thenAnswer((Answer<Connection>) answer -> mockConnection);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetConnectionWithEmptyMap() {
        Map<String, Object> emptyMap = Maps.newHashMap();
        HBaseConfig hBaseConfig = new HBaseConfig();
        hBaseConfig.setHbaseConfig(emptyMap);
        HBaseHelper.getHbaseConnection(hBaseConfig, null, null);
    }

    @Test
    public void testGetConnectionWithoutKerberos() {
        final String k1 = "where?";
        final String v1 = "I'm on a boat";
        final String k2 = "when?";
        final String v2 = "midnight";
        final String k3 = "why?";
        final String v3 = "what do you think?";
        final String k4 = "which way?";
        final String v4 = "south, always south...";

        HBaseConfig hBaseConfig = new HBaseConfig();

        Map<String, Object> confMap = Maps.newHashMap();
        confMap.put(k1, v1);
        confMap.put(k2, v2);
        confMap.put(k3, v3);
        confMap.put(k4, v4);

        hBaseConfig.setHbaseConfig(confMap);

        Assert.assertEquals(
                mockConnection, HBaseHelper.getHbaseConnection(hBaseConfig, null, null));
    }

    @Test(expected = RuntimeException.class)
    public void testGetConnectionWithKerberosThenMissPrincipal() {
        Map<String, Object> confMap = Maps.newHashMap();
        HBaseConfig hBaseConfig = new HBaseConfig();

        confMap.put(KEY_HBASE_SECURITY_AUTHORIZATION, KRB_STR);

        hBaseConfig.setHbaseConfig(confMap);
        HBaseHelper.getHbaseConnection(hBaseConfig, null, null);
    }

    @Test
    public void testConvertRowKeyWithBlankRowKey() {
        String emptyRowKey = "";
        String rowKey = "test_rowKey";

        Assert.assertEquals(
                HConstants.EMPTY_BYTE_ARRAY, HBaseHelper.convertRowKey(emptyRowKey, false));

        Assert.assertEquals(rowKey, new String(HBaseHelper.convertRowKey(rowKey, true)));
        Assert.assertEquals(rowKey, new String(HBaseHelper.convertRowKey(rowKey, false)));
    }

    @Test
    public void testCloseConnection() {
        Connection mockConnection = mock(Connection.class);
        HBaseHelper.closeConnection(mockConnection);
    }

    @Test
    public void testCloseHBaseAdmin() {
        Admin mockAdmin = mock(Admin.class);
        HBaseHelper.closeAdmin(mockAdmin);
    }

    @Test
    public void testCloseRegionLocator() {
        RegionLocator mockRegionLocator = mock(RegionLocator.class);
        HBaseHelper.closeRegionLocator(mockRegionLocator);
    }

    @Test
    public void testCloseBufferedMutator() {
        BufferedMutator mockMutator = mock(BufferedMutator.class);
        HBaseHelper.closeBufferedMutator(mockMutator);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckNonexistentTable() throws IOException {
        Admin mockAdmin = mock(Admin.class);
        TableName mockTableName = mock(TableName.class);

        when(mockAdmin.tableExists(mockTableName)).thenReturn(false);
        HBaseHelper.checkHbaseTable(mockAdmin, mockTableName);
    }

    @Test(expected = RuntimeException.class)
    public void testCheckUnavailableTable() throws IOException {
        Admin mockAdmin = mock(Admin.class);
        TableName mockTableName = mock(TableName.class);

        when(mockAdmin.tableExists(mockTableName)).thenReturn(true);
        when(mockAdmin.isTableAvailable(mockTableName)).thenReturn(false);
        HBaseHelper.checkHbaseTable(mockAdmin, mockTableName);
    }

    @Test(expected = RuntimeException.class)
    public void testCheckDisabledTable() throws IOException {
        Admin mockAdmin = mock(Admin.class);
        TableName mockTableName = mock(TableName.class);

        when(mockAdmin.tableExists(mockTableName)).thenReturn(true);
        when(mockAdmin.isTableAvailable(mockTableName)).thenReturn(true);
        when(mockAdmin.isTableDisabled(mockTableName)).thenReturn(true);
        HBaseHelper.checkHbaseTable(mockAdmin, mockTableName);
    }
}
