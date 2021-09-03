/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dtstack.flinkx.connector.sqlservercdc.entity;

import java.util.List;
import java.util.Objects;

/**
 * Date: 2019/12/03 Company: www.dtstack.com
 *
 * <p>this class is copied from (https://github.com/debezium/debezium). but there are some different
 * from the origin.
 *
 * @author tudou
 */
public class ChangeTable {

    private static final String CDC_SCHEMA = "cdc";
    private final String captureInstance;
    private final TableId sourceTableId;
    private final TableId changeTableId;
    private final Lsn startLsn;
    private Lsn stopLsn;
    private List<String> columnList;
    private final int changeTableObjectId;

    public ChangeTable(
            TableId sourceTableId,
            String captureInstance,
            int changeTableObjectId,
            Lsn startLsn,
            Lsn stopLsn,
            List<String> columnList) {
        super();
        this.sourceTableId = sourceTableId;
        this.captureInstance = captureInstance;
        this.changeTableObjectId = changeTableObjectId;
        this.startLsn = startLsn;
        this.stopLsn = stopLsn;
        this.columnList = columnList;
        this.changeTableId =
                sourceTableId != null
                        ? new TableId(
                                sourceTableId.getCatalogName(), CDC_SCHEMA, captureInstance + "_CT")
                        : null;
    }

    public String getCaptureInstance() {
        return captureInstance;
    }

    public Lsn getStartLsn() {
        return startLsn;
    }

    public Lsn getStopLsn() {
        return stopLsn;
    }

    public void setStopLsn(Lsn stopLsn) {
        this.stopLsn = stopLsn;
    }

    public TableId getSourceTableId() {
        return sourceTableId;
    }

    public List<String> getColumnList() {
        return columnList;
    }

    @Override
    public String toString() {
        return "ChangeTable{"
                + "captureInstance='"
                + captureInstance
                + '\''
                + ", sourceTableId="
                + sourceTableId
                + ", changeTableId="
                + changeTableId
                + ", startLsn="
                + startLsn
                + ", stopLsn="
                + stopLsn
                + ", columnList="
                + columnList
                + ", changeTableObjectId="
                + changeTableObjectId
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChangeTable that = (ChangeTable) o;
        return changeTableObjectId == that.changeTableObjectId
                && Objects.equals(captureInstance, that.captureInstance)
                && Objects.equals(sourceTableId, that.sourceTableId)
                && Objects.equals(changeTableId, that.changeTableId)
                && Objects.equals(startLsn, that.startLsn)
                && Objects.equals(stopLsn, that.stopLsn)
                && Objects.equals(columnList, that.columnList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                captureInstance,
                sourceTableId,
                changeTableId,
                startLsn,
                stopLsn,
                columnList,
                changeTableObjectId);
    }
}
