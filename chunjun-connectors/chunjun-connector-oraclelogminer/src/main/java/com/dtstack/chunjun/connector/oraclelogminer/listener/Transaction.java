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

import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.List;

/**
 * Date: 2020/08/13 Company: www.dtstack.com
 *
 * @author dujie
 */
public class Transaction implements Serializable {
    private static final long serialVersionUID = 1L;

    private BigInteger scn;

    private List<RecordLog> recordLogs;

    public Transaction(BigInteger scn, List<RecordLog> recordLogs) {
        this.scn = scn;
        this.recordLogs = recordLogs;
    }

    public BigInteger getScn() {
        return scn;
    }

    public void setScn(BigInteger scn) {
        this.scn = scn;
    }

    public void addRecord(RecordLog recordLog) {
        recordLogs.add(recordLog);
    }

    public boolean isEmpty() {
        return CollectionUtils.isEmpty(this.recordLogs);
    }

    public RecordLog getLast() {
        return recordLogs.get(recordLogs.size() - 1);
    }

    public void remove(RecordLog recordLog) {
        recordLogs.remove(recordLog);
    }

    public void removeLast() {
        recordLogs.remove(recordLogs.size() - 1);
    }
}
