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

import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.factory.ChunJunThreadFactory;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractAllTableFunction extends LookupFunction {

    private static final long serialVersionUID = 5565390751716048922L;
    /** 和维表join字段的名称 */
    protected final String[] keyNames;
    /** 缓存 */
    protected AtomicReference<Object> cacheRef = new AtomicReference<>();
    /** 定时加载 */
    private ScheduledExecutorService es;
    /** 维表配置 */
    protected final LookupConfig lookupConfig;
    /** 字段名称 */
    protected final String[] fieldsName;
    /** 数据类型转换器 */
    protected final AbstractRowConverter rowConverter;

    protected final RowData.FieldGetter[] fieldGetters;

    public AbstractAllTableFunction(
            String[] fieldNames,
            String[] keyNames,
            LookupConfig lookupConfig,
            AbstractRowConverter rowConverter) {
        this.keyNames = keyNames;
        this.lookupConfig = lookupConfig;
        this.fieldsName = fieldNames;
        this.rowConverter = rowConverter;
        this.fieldGetters = new RowData.FieldGetter[keyNames.length];
        List<Pair<LogicalType, Integer>> fieldTypeAndPositionOfKeyField =
                getFieldTypeAndPositionOfKeyField(keyNames, rowConverter.getRowType());
        for (int i = 0; i < fieldTypeAndPositionOfKeyField.size(); i++) {
            Pair<LogicalType, Integer> typeAndPosition = fieldTypeAndPositionOfKeyField.get(i);
            fieldGetters[i] =
                    RowData.createFieldGetter(
                            typeAndPosition.getLeft(), typeAndPosition.getRight());
        }
    }

    protected List<Pair<LogicalType, Integer>> getFieldTypeAndPositionOfKeyField(
            String[] keyNames, RowType rowType) {
        List<Pair<LogicalType, Integer>> typeAndPosition = Lists.newLinkedList();
        for (int i = 0; i < keyNames.length; i++) {
            LogicalType type = rowType.getTypeAt(rowType.getFieldIndex(keyNames[i]));
            typeAndPosition.add(Pair.of(type, i));
        }
        return typeAndPosition;
    }

    /** 初始化加载数据库中数据 */
    protected void initCache() {
        Map<String, List<Map<String, Object>>> newCache = Maps.newConcurrentMap();
        cacheRef.set(newCache);
        loadData(newCache);
    }

    /** 定时加载数据库中数据 */
    protected void reloadCache() {
        // reload cacheRef and replace to old cacheRef
        Map<String, List<Map<String, Object>>> newCache = Maps.newConcurrentMap();
        try {
            loadData(newCache);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        cacheRef.set(newCache);
        log.info(
                "----- " + lookupConfig.getTableName() + ": all cacheRef reload end:{}",
                LocalDateTime.now());
    }

    /**
     * 加载数据到缓存
     *
     * @param cacheRef
     */
    protected abstract void loadData(Object cacheRef);

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        initCache();
        log.info("----- all cacheRef init end-----");

        // start reload cache thread
        es = new ScheduledThreadPoolExecutor(1, new ChunJunThreadFactory("cache-all-reload"));
        es.scheduleAtFixedRate(
                this::reloadCache,
                lookupConfig.getPeriod(),
                lookupConfig.getPeriod(),
                TimeUnit.MILLISECONDS);
    }

    /**
     * 缓存一行数据
     *
     * @param oneRow 一行数据
     * @param tmpCache 缓存的数据<key ,list<value>>
     */
    protected void buildCache(
            Map<String, Object> oneRow, Map<String, List<Map<String, Object>>> tmpCache) {

        String cacheKey =
                new ArrayList<>(Arrays.asList(keyNames))
                        .stream()
                                .map(oneRow::get)
                                .map(String::valueOf)
                                .collect(Collectors.joining("_"));

        tmpCache.computeIfAbsent(cacheKey, key -> Lists.newArrayList()).add(oneRow);
    }

    /**
     * 每条数据都会进入该方法
     *
     * @param keyRow 维表join key的值
     */
    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        List<String> dataList = Lists.newLinkedList();
        List<RowData> hitRowData = Lists.newArrayList();
        for (int i = 0; i < keyRow.getArity(); i++) {
            dataList.add(String.valueOf(fieldGetters[i].getFieldOrNull(keyRow)));
        }
        String cacheKey = String.join("_", dataList);
        List<Map<String, Object>> cacheList =
                ((Map<String, List<Map<String, Object>>>) (cacheRef.get())).get(cacheKey);
        // 有数据才往下发，(左/内)连接flink会做相应的处理
        if (!CollectionUtils.isEmpty(cacheList)) {
            cacheList.forEach(one -> hitRowData.add(fillData(one)));
        }

        return hitRowData;
    }

    public RowData fillData(Object sideInput) {
        Map<String, Object> cacheInfo = (Map<String, Object>) sideInput;
        GenericRowData row = new GenericRowData(fieldsName.length);
        for (int i = 0; i < fieldsName.length; i++) {
            row.setField(i, cacheInfo.get(fieldsName[i]));
        }
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    /** 资源释放 */
    @Override
    public void close() throws Exception {
        if (null != es && !es.isShutdown()) {
            es.shutdown();
        }
    }
}
