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

package com.dtstack.chunjun.connector.oraclelogminer.listener;

import com.dtstack.chunjun.connector.oraclelogminer.entity.RecordLog;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** 事务管理器 监听的DML语句缓存，在commit/rollback时删除 */
@Slf4j
public class TransactionManager {

    /** 缓存的结构为 xidUsn+xidSLt+xidSqn(事务id),当前事务内数据 */
    private final Cache<String, LinkedList<RecordLog>> recordCache;

    /**
     * 缓存的结构为 xidUsn+xidSLt+xidSqn(事务id),最后一次处理回滚数据对应的业务操作scn以及rowid
     * 回滚事务id以及最新回滚数据对应的业务操作scn和rowid的对应关系
     */
    private final Map<String, Pair<BigInteger, String>> earliestResolveOperateForRollback;

    private final Long eventSize;

    public TransactionManager(Long transactionSize, Long eventSize, long transactionExpireTime) {
        this.recordCache =
                CacheBuilder.newBuilder()
                        .maximumSize(transactionSize)
                        .expireAfterWrite(transactionExpireTime, TimeUnit.MINUTES)
                        .build();
        this.eventSize = eventSize;
        this.earliestResolveOperateForRollback = new HashMap<>();
    }

    public void putCache(RecordLog recordLog) {
        // 缓存里不放入delete的DML语句
        if (recordLog.getOperationCode() == 2) {
            return;
        }
        String key = recordLog.getXidUsn() + recordLog.getXidSlt() + recordLog.getXidSqn();
        LinkedList<RecordLog> recordList = recordCache.getIfPresent(key);
        if (Objects.isNull(recordList)) {
            LinkedList<RecordLog> data = new LinkedList<>();
            recordCache.put(key, data);
            recordList = data;
        }

        recordLog.setSqlUndo(recordLog.getSqlUndo().replace("IS NULL", "= NULL"));
        recordLog.setSqlRedo(recordLog.getSqlRedo().replace("IS NULL", "= NULL"));
        recordList.add(recordLog);
        if (recordList.size() > eventSize) {
            recordList.removeFirst();
        }
    }

    /** 清理已提交事务的缓存 */
    public void cleanCache(String xidUsn, String xidSLt, String xidSqn) {
        String txId = xidUsn + xidSLt + xidSqn;
        log.debug(
                "clean secondKeyCache，xidSqn = {}, xidUsn = {} ,xidSLt = {} ",
                xidSqn,
                xidUsn,
                xidSLt);
        recordCache.invalidate(txId);
        earliestResolveOperateForRollback.remove(xidUsn + xidSLt + xidSqn);
        log.debug("after clean，current recordCache size = {}", recordCache.size());
    }

    /** 从缓存的dml语句里找到rollback语句对应的DML语句 如果查找到 需要删除对应的缓存信息 */
    public RecordLog queryUndoLogFromCache(String xidUsn, String xidSlt, String xidSqn) {
        String key = xidUsn + xidSlt + xidSqn;
        LinkedList<RecordLog> recordLogs = recordCache.getIfPresent(key);
        if (CollectionUtils.isEmpty(recordLogs)) {
            return null;
        }
        RecordLog recordLog = recordLogs.removeLast();
        earliestResolveOperateForRollback.put(
                key, Pair.of(recordLog.getScn(), recordLog.getRowId()));
        return recordLog;
    }

    public Pair<BigInteger, String> getEarliestRollbackOperation(
            String xidUsn, String xidSlt, String xidSqn) {
        return earliestResolveOperateForRollback.get(xidUsn + xidSlt + xidSqn);
    }
}
