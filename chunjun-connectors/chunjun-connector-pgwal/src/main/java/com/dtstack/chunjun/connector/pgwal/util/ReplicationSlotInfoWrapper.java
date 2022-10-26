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

package com.dtstack.chunjun.connector.pgwal.util;

import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.ReplicationSlotInfo;
import org.postgresql.replication.ReplicationType;

public class ReplicationSlotInfoWrapper {

    private final boolean active;
    private final Boolean isTemp;
    private ReplicationSlotInfo slotInfo;

    public ReplicationSlotInfoWrapper(String slotName, String active, String lsn) {
        this.slotInfo =
                new ReplicationSlotInfo(
                        slotName,
                        ReplicationType.LOGICAL,
                        LogSequenceNumber.valueOf(lsn),
                        null,
                        "pgoutput");
        this.active = "t".equalsIgnoreCase(active);
        this.isTemp = false;
    }

    public ReplicationSlotInfoWrapper(ReplicationSlotInfo replicationSlotInfo, Boolean isTemp) {
        this.slotInfo = replicationSlotInfo;
        this.active = true;
        this.isTemp = isTemp;
    }

    public boolean isActive() {
        return active;
    }

    public String getSlotName() {
        return slotInfo.getSlotName();
    }
}
