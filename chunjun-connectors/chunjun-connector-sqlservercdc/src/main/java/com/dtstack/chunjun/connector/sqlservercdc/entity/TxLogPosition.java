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

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dtstack.chunjun.connector.sqlservercdc.entity;

public class TxLogPosition implements Comparable<TxLogPosition> {
    public static final TxLogPosition NULL = new TxLogPosition(null, null);
    private final Lsn commitLsn;
    private final Lsn inTxLsn;

    private TxLogPosition(Lsn commitLsn, Lsn inTxLsn) {
        this.commitLsn = commitLsn;
        this.inTxLsn = inTxLsn;
    }

    public Lsn getCommitLsn() {
        return commitLsn;
    }

    public Lsn getInTxLsn() {
        return inTxLsn;
    }

    @Override
    public String toString() {
        return this == NULL ? "NULL" : commitLsn + "(" + inTxLsn + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((commitLsn == null) ? 0 : commitLsn.hashCode());
        result = prime * result + ((inTxLsn == null) ? 0 : inTxLsn.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TxLogPosition other = (TxLogPosition) obj;
        if (commitLsn == null) {
            if (other.commitLsn != null) {
                return false;
            }
        } else if (!commitLsn.equals(other.commitLsn)) {
            return false;
        }
        if (inTxLsn == null) {
            if (other.inTxLsn != null) {
                return false;
            }
        } else if (!inTxLsn.equals(other.inTxLsn)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(TxLogPosition o) {
        final int comparison = commitLsn.compareTo(o.getCommitLsn());
        return comparison == 0 ? inTxLsn.compareTo(o.inTxLsn) : comparison;
    }

    public static TxLogPosition valueOf(Lsn commitLsn, Lsn inTxLsn) {
        return commitLsn == null && inTxLsn == null
                ? NULL
                : new TxLogPosition(
                        commitLsn == null ? Lsn.NULL : commitLsn,
                        inTxLsn == null ? Lsn.NULL : inTxLsn);
    }

    public static TxLogPosition valueOf(Lsn commitLsn) {
        return valueOf(commitLsn, Lsn.NULL);
    }

    public boolean isAvailable() {
        return inTxLsn != null && commitLsn != null;
    }
}
