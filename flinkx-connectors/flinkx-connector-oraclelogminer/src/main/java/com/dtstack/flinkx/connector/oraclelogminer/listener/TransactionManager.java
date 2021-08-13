/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.connector.oraclelogminer.listener;

import com.dtstack.flinkx.connector.oraclelogminer.entity.RecordLog;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class TransactionManager {

    public static Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

    /** 缓存的结构为 xidUsn+xidSLt+xidSqn(事务id)+rowId,scn* */
    private final Cache<String, LinkedHashMap<BigDecimal, LinkedList<RecordLog>>> recordCache;

    /**
     * recordCache的二级缓存，缓存二级key，结构为： xidUsn+xidSLt+xidSqn(事务id)， xidUsn+xidSLt+xidSqn(事务id)+rowId列表
     * *
     */
    private final Cache<String, LinkedList<String>> secondKeyCache;

    public TransactionManager(Long transactionSize, long transactionExpireTime) {
        this.recordCache =
                CacheBuilder.newBuilder()
                        .maximumSize(transactionSize)
                        .expireAfterWrite(transactionExpireTime, TimeUnit.MINUTES)
                        .build();
        this.secondKeyCache =
                CacheBuilder.newBuilder()
                        .maximumSize(transactionSize)
                        .expireAfterWrite(transactionExpireTime, TimeUnit.MINUTES)
                        .build();
    }

    /**
     * insert以及update record放入缓存中
     *
     * @param recordLog
     */
    public void putCache(RecordLog recordLog) {
        // 缓存里不放入delete的DML语句
        if (recordLog.getOperationCode() == 2) {
            return;
        }
        String key =
                recordLog.getXidUsn()
                        + recordLog.getXidSlt()
                        + recordLog.getXidSqn()
                        + recordLog.getRowId();
        Map<BigDecimal, LinkedList<RecordLog>> bigDecimalListMap = recordCache.getIfPresent(key);
        if (Objects.isNull(bigDecimalListMap)) {
            LinkedHashMap<BigDecimal, LinkedList<RecordLog>> data = new LinkedHashMap<>(32);
            recordCache.put(key, data);
            bigDecimalListMap = data;
        }
        LinkedList<RecordLog> recordList = bigDecimalListMap.get(recordLog.getScn());
        if (CollectionUtils.isEmpty(recordList)) {
            recordList = new LinkedList<>();
            bigDecimalListMap.put(recordLog.getScn(), recordList);
        }

        recordLog.setSqlUndo(recordLog.getSqlUndo().replace("IS NULL", "= NULL"));
        recordLog.setSqlRedo(recordLog.getSqlRedo().replace("IS NULL", "= NULL"));
        recordList.add(recordLog);

        String txId = recordLog.getXidUsn() + recordLog.getXidSlt() + recordLog.getXidSqn();
        LinkedList<String> keyList = secondKeyCache.getIfPresent(txId);
        if (Objects.isNull(keyList)) {
            keyList = new LinkedList<>();
        }
        keyList.add(key);
        LOG.debug(
                "add cache，XidSqn = {}, RowId = {}, recordLog = {}",
                recordLog.getXidSqn(),
                recordLog.getRowId(),
                recordLog);
        secondKeyCache.put(txId, keyList);
        LOG.debug(
                "after add，recordCache size = {}, secondKeyCache size = {}",
                recordCache.size(),
                secondKeyCache.size());
    }

    /** 清理已提交事务的缓存 */
    public void cleanCache(String xidUsn, String xidSLt, String xidSqn) {
        String txId = xidUsn + xidSLt + xidSqn;
        LinkedList<String> keyList = secondKeyCache.getIfPresent(txId);
        if (Objects.isNull(keyList)) {
            return;
        }
        for (String key : keyList) {
            LOG.debug("clean recordCache，key = {}", key);
            recordCache.invalidate(key);
        }
        LOG.debug(
                "clean secondKeyCache，xidSqn = {}, xidUsn = {} ,xidSLt = {} ",
                xidSqn,
                xidUsn,
                xidSLt);
        secondKeyCache.invalidate(txId);

        LOG.debug(
                "after clean，recordCache size = {}, secondKeyCache size = {}",
                recordCache.size(),
                secondKeyCache.size());
    }

    /**
     * 从缓存的dml语句里找到rollback语句对应的DML语句 如果查找到 需要删除对应的缓存信息
     *
     * @param xidUsn
     * @param xidSlt
     * @param xidSqn
     * @param rowId
     * @param scn scn of rollback
     * @return dml Log
     */
    public RecordLog queryUndoLogFromCache(
            String xidUsn, String xidSlt, String xidSqn, String rowId, BigDecimal scn) {
        String key = xidUsn + xidSlt + xidSqn + rowId;
        LinkedHashMap<BigDecimal, LinkedList<RecordLog>> recordMap = recordCache.getIfPresent(key);
        if (MapUtils.isEmpty(recordMap)) {
            return null;
        }
        // 根据scn号查找 如果scn号相同 则取此对应的最后DML语句  dml按顺序添加，rollback倒序取对应的语句
        LinkedList<RecordLog> recordLogList = recordMap.get(scn);
        BigDecimal recordKey = scn;
        RecordLog recordLog = null;
        if (CollectionUtils.isEmpty(recordLogList)) {
            // 如果scn相同的DML语句没有 则取同一个事务里rowId相同的最后一个
            Iterator<Map.Entry<BigDecimal, LinkedList<RecordLog>>> iterator =
                    recordMap.entrySet().iterator();
            Map.Entry<BigDecimal, LinkedList<RecordLog>> tail = null;
            while (iterator.hasNext()) {
                tail = iterator.next();
            }
            if (Objects.nonNull(tail)) {
                recordLogList = tail.getValue();
                recordKey = tail.getKey();
            }
        }
        if (CollectionUtils.isNotEmpty(recordLogList)) {
            recordLog = recordLogList.getLast();
            recordLogList.removeLast();
            LOG.info("query a insert sql for rollback in cache,rollback scn is {}", scn);
        }

        if (CollectionUtils.isEmpty(recordLogList)) {
            recordMap.remove(recordKey);
        }

        if (recordMap.isEmpty()) {
            recordCache.invalidate(key);
        }

        secondKeyCache.invalidate(xidUsn + xidSlt + xidSqn);

        return recordLog;
    }
}
