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
