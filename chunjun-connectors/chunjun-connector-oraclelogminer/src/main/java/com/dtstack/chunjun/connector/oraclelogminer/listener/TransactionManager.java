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
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Date: 2021/08/13 Company: www.dtstack.com
 *
 * @author dujie
 *     <p>事务管理器 监听的DML语句缓存，在commit/rollback时删除
 */
public class TransactionManager {

    public static Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

    /** 缓存的结构为 xidUsn+xidSLt+xidSqn(事务id)+rowId,Transaction* */
    private final Cache<String, List<Transaction>> recordCache;

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

        recordLog.setSqlUndo(recordLog.getSqlUndo().replace("IS NULL", "= NULL"));
        recordLog.setSqlRedo(recordLog.getSqlRedo().replace("IS NULL", "= NULL"));

        String key =
                recordLog.getXidUsn()
                        + recordLog.getXidSlt()
                        + recordLog.getXidSqn()
                        + recordLog.getRowId();
        List<Transaction> transactionList = recordCache.getIfPresent(key);
        if (CollectionUtils.isEmpty(transactionList)) {
            List<Transaction> data = new ArrayList<>(32);
            recordCache.put(key, data);
            transactionList = data;
        }
        Optional<Transaction> transaction =
                transactionList.stream()
                        .filter(i -> i.getScn().compareTo(recordLog.getScn()) == 0)
                        .findFirst();

        if (!transaction.isPresent()) {
            transactionList.add(new Transaction(recordLog.getScn(), Lists.newArrayList(recordLog)));
        } else {
            transaction.get().addRecord(recordLog);
        }

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
            String xidUsn, String xidSlt, String xidSqn, String rowId, BigInteger scn) {
        String key = xidUsn + xidSlt + xidSqn + rowId;
        List<Transaction> transactionList = recordCache.getIfPresent(key);
        if (CollectionUtils.isEmpty(transactionList)) {
            return null;
        }
        // 根据scn号查找 如果scn号相同 则取此对应的最后DML语句  dml按顺序添加，rollback倒序取对应的语句
        Optional<Transaction> transactionOptional =
                transactionList.stream().filter(i -> i.getScn().compareTo(scn) == 0).findFirst();
        // 如果scn相同的DML语句没有 则取同一个事务里rowId相同的最后一个
        Transaction transaction =
                transactionOptional.orElse(transactionList.get(transactionList.size() - 1));
        RecordLog recordLog = null;

        if (!transaction.isEmpty()) {
            recordLog = transaction.getLast();
            transaction.removeLast();
            LOG.info("query a insert sql for rollback in cache,rollback scn is {}", scn);
        }

        if (transaction.isEmpty()) {
            transactionList.remove(transaction);
        }

        if (transactionList.isEmpty()) {
            recordCache.invalidate(key);
        }

        secondKeyCache.invalidate(xidUsn + xidSlt + xidSqn);

        return recordLog;
    }
}
