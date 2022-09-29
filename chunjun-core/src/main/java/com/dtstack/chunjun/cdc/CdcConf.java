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

package com.dtstack.chunjun.cdc;

import com.dtstack.chunjun.cdc.conf.CacheConf;
import com.dtstack.chunjun.cdc.conf.DDLConf;

import java.io.Serializable;
import java.util.StringJoiner;

public class CdcConf implements Serializable {

    private static final long serialVersionUID = 1L;

    /** whether skip ddl statement or not. */
    private boolean skipDDL = true;

    /** worker的核心线程数 */
    private int workerNum = 2;

    /** worker遍历队列时的步长 */
    private int workerSize = 3;

    /** worker线程池的最大容量 */
    private int workerMax = 3;

    private int bathSize = 1000;

    private long maxBytes = 1024 * 1024 * 1024;

    private long cacheTimeout = 60;

    private DDLConf ddl;

    private CacheConf cache;

    public int getWorkerNum() {
        return workerNum;
    }

    public void setWorkerNum(int workerNum) {
        this.workerNum = workerNum;
    }

    public int getWorkerSize() {
        return workerSize;
    }

    public void setWorkerSize(int workerSize) {
        this.workerSize = workerSize;
    }

    public int getWorkerMax() {
        return workerMax;
    }

    public void setWorkerMax(int workerMax) {
        this.workerMax = workerMax;
    }

    public boolean isSkipDDL() {
        return skipDDL;
    }

    public void setSkipDDL(boolean skipDDL) {
        this.skipDDL = skipDDL;
    }

    public int getBathSize() {
        return bathSize;
    }

    public void setBathSize(int bathSize) {
        this.bathSize = bathSize;
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    public void setMaxBytes(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    public long getCacheTimeout() {
        return cacheTimeout;
    }

    public void setCacheTimeout(long cacheTimeout) {
        this.cacheTimeout = cacheTimeout;
    }

    public DDLConf getDdl() {
        return ddl;
    }

    public void setDdl(DDLConf ddl) {
        this.ddl = ddl;
    }

    public CacheConf getCache() {
        return cache;
    }

    public void setCache(CacheConf cache) {
        this.cache = cache;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CdcConf.class.getSimpleName() + "[", "]")
                .add("skipDDL=" + skipDDL)
                .add("workerNum=" + workerNum)
                .add("workerSize=" + workerSize)
                .add("workerMax=" + workerMax)
                .add("bathSize=" + bathSize)
                .add("maxBytes=" + maxBytes)
                .add("ddl=" + ddl)
                .add("cache=" + cache)
                .toString();
    }
}
