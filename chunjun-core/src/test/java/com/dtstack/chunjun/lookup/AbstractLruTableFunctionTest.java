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

package com.dtstack.chunjun.lookup;

import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.enums.ECacheContentType;
import com.dtstack.chunjun.lookup.cache.CacheObj;
import com.dtstack.chunjun.lookup.config.LookupConfig;
import com.dtstack.chunjun.lookup.config.LookupConfigFactory;
import com.dtstack.chunjun.source.format.MockInputFormat;
import com.dtstack.chunjun.source.format.MockRowConverter;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.taskexecutor.rpc.RpcGlobalAggregateManager;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractLruTableFunctionTest {

    private AbstractLruTableFunction lruTableFunction;

    @BeforeEach
    public void setup() {
        LookupConfig lookupConfig = new LookupConfig();
        MockRowConverter mockRowConverter = new MockRowConverter();
        this.lruTableFunction = new MockLruTableFunction(lookupConfig, mockRowConverter);
    }

    @Test
    public void testOpen() throws Exception {
        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setTaskName("no")
                        .setExecutionConfig(new ExecutionConfig())
                        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
                        .setTaskManagerRuntimeInfo(
                                new MockInputFormat.MockTaskManagerConfiguration())
                        .setAggregateManager(
                                new RpcGlobalAggregateManager(
                                        new TestingJobMasterGatewayBuilder().build()))
                        .build();
        MockInputFormat.MockRuntimeContext context =
                new MockInputFormat.MockRuntimeContext(environment);
        FunctionContext functionContext = new FunctionContext(context);
        lruTableFunction.open(functionContext);
    }

    @Test
    @DisplayName("when cache type is none, initCache should do nothing and sideCache is null")
    public void testInitCacheWhenCacheTypeIsNone() {
        LookupConfig lookupConfig = LookupConfigFactory.createLookupConfig(new Configuration());
        lookupConfig.setCache(CacheType.NONE.name());
        lruTableFunction.lookupConfig = lookupConfig;
        lruTableFunction.initCache();
        assertNull(lruTableFunction.sideCache);
    }

    @Test
    @DisplayName("when cache type is all, initCache should throw RuntimeException")
    public void testInitCacheWhenCacheTypeIsAll() {
        LookupConfig lookupConfig = LookupConfigFactory.createLookupConfig(new Configuration());
        lookupConfig.setCache(CacheType.ALL.name());
        lruTableFunction.lookupConfig = lookupConfig;
        RuntimeException thrown =
                assertThrows(
                        RuntimeException.class,
                        () -> lruTableFunction.initCache(),
                        "Expected initCache() to throw, but it didn't");
        assertTrue(thrown.getMessage().contains("not support side cache with type"));
    }

    @Test
    @DisplayName("when cache type is lru, initCache should init sideCache")
    public void testInitCacheWhenCacheTypeIsLru() {
        LookupConfig lookupConfig = LookupConfigFactory.createLookupConfig(new Configuration());
        lookupConfig.setCache(CacheType.LRU.name());
        lruTableFunction.lookupConfig = lookupConfig;
        lruTableFunction.initCache();
        assertNotNull(lruTableFunction.sideCache);
    }

    @Test
    public void testInitMetric() {
        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setTaskName("no")
                        .setExecutionConfig(new ExecutionConfig())
                        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
                        .setTaskManagerRuntimeInfo(
                                new MockInputFormat.MockTaskManagerConfiguration())
                        .setAggregateManager(
                                new RpcGlobalAggregateManager(
                                        new TestingJobMasterGatewayBuilder().build()))
                        .build();
        MockInputFormat.MockRuntimeContext context =
                new MockInputFormat.MockRuntimeContext(environment);
        FunctionContext functionContext = new FunctionContext(context);
        lruTableFunction.initMetric(functionContext);
        assertNotNull(lruTableFunction.parseErrorRecords);
    }

    @Test
    public void testDealMissKey() throws ExecutionException, InterruptedException {
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        lruTableFunction.dealMissKey(future);
        Collection<RowData> data = future.get();
        assertTrue(data.isEmpty());
    }

    @Test
    public void testDealCacheData() {
        LookupConfig lookupConfig = LookupConfigFactory.createLookupConfig(new Configuration());
        lookupConfig.setCache(CacheType.LRU.name());
        lruTableFunction.lookupConfig = lookupConfig;
        lruTableFunction.initCache();
        CacheObj cacheObjA = CacheObj.buildCacheObj(ECacheContentType.SingleLine, "");
        lruTableFunction.dealCacheData("a", cacheObjA);
        CacheObj cacheObjB = lruTableFunction.sideCache.getFromCache("a");
        assertEquals(cacheObjA, cacheObjB);
    }

    @Test
    @DisplayName("it should return empty list when timeOutNum is not large than error limit")
    public void testTimeoutButNotLargeThanErrorLimit()
            throws ExecutionException, InterruptedException {
        lruTableFunction.lookupConfig.setErrorLimit(100);
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        lruTableFunction.timeout(future, "a");
        Collection<RowData> data = future.get();
        assertTrue(data.isEmpty());
    }

    @Test
    @DisplayName("it should return empty list when timeOutNum is large than error limit")
    public void testTimeoutAndLargeThanErrorLimit()
            throws ExecutionException, InterruptedException {
        lruTableFunction.lookupConfig.setErrorLimit(0);
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        lruTableFunction.timeout(future, "a");
        ExecutionException thrown =
                assertThrows(
                        ExecutionException.class,
                        future::get,
                        "Expected timeout() to throw, but it didn't");
        assertTrue(
                thrown.getCause()
                        .getCause()
                        .getMessage()
                        .contains("Async function call timedOutNum beyond limit"));
    }

    @Test
    public void testBuildCacheKey() {
        String cacheKey = lruTableFunction.buildCacheKey("a", "b");
        assertEquals("a_b", cacheKey);
    }

    @Test
    public void testDealFillDataErrorButNotLargeThanErrorLimit()
            throws ExecutionException, InterruptedException {
        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setTaskName("no")
                        .setExecutionConfig(new ExecutionConfig())
                        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
                        .setTaskManagerRuntimeInfo(
                                new MockInputFormat.MockTaskManagerConfiguration())
                        .setAggregateManager(
                                new RpcGlobalAggregateManager(
                                        new TestingJobMasterGatewayBuilder().build()))
                        .build();
        MockInputFormat.MockRuntimeContext context =
                new MockInputFormat.MockRuntimeContext(environment);
        FunctionContext functionContext = new FunctionContext(context);
        lruTableFunction.initMetric(functionContext);
        lruTableFunction.lookupConfig.setErrorLimit(100);
        long oldData = lruTableFunction.parseErrorRecords.getCount();
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        RuntimeException e = new RuntimeException("error");
        lruTableFunction.dealFillDataError(future, e);
        long newData = lruTableFunction.parseErrorRecords.getCount();
        assertEquals(1, newData - oldData);
        Collection<RowData> data = future.get();
        assertTrue(data.isEmpty());
    }

    @Test
    public void testDealFillDataErrorAndLargeThanErrorLimit()
            throws ExecutionException, InterruptedException {
        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setTaskName("no")
                        .setExecutionConfig(new ExecutionConfig())
                        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
                        .setTaskManagerRuntimeInfo(
                                new MockInputFormat.MockTaskManagerConfiguration())
                        .setAggregateManager(
                                new RpcGlobalAggregateManager(
                                        new TestingJobMasterGatewayBuilder().build()))
                        .build();
        MockInputFormat.MockRuntimeContext context =
                new MockInputFormat.MockRuntimeContext(environment);
        FunctionContext functionContext = new FunctionContext(context);
        lruTableFunction.initMetric(functionContext);
        lruTableFunction.lookupConfig.setErrorLimit(0);
        long oldData = lruTableFunction.parseErrorRecords.getCount();
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        RuntimeException e = new RuntimeException("error");
        lruTableFunction.dealFillDataError(future, e);
        long newData = lruTableFunction.parseErrorRecords.getCount();
        assertEquals(1, newData - oldData);
        ExecutionException thrown =
                assertThrows(
                        ExecutionException.class,
                        future::get,
                        "Expected dealFillDataError() to throw, but it didn't");
        assertTrue(thrown.getCause().getCause().getMessage().contains("error"));
    }
}
