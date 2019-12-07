/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dtstack.flinkx.sqlservercdc;

import java.util.List;

/**
 * Date: 2019/12/03
 * Company: www.dtstack.com
 * <p>
 * this class is copied from (https://github.com/debezium/debezium).
 * but there are some different from the origin.
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

    public ChangeTable(TableId sourceTableId, String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn, List<String> columnList) {
        super();
        this.sourceTableId = sourceTableId;
        this.captureInstance = captureInstance;
        this.changeTableObjectId = changeTableObjectId;
        this.startLsn = startLsn;
        this.stopLsn = stopLsn;
        this.columnList = columnList;
        this.changeTableId = sourceTableId != null ? new TableId(sourceTableId.getCatalogName(), CDC_SCHEMA, captureInstance + "_CT") : null;
    }

    public ChangeTable(String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn, List<String> columnList) {
        this(null, captureInstance, changeTableObjectId, startLsn, stopLsn, columnList);
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

    public void setColumnList(List<String> columnList) {
        this.columnList = columnList;
    }

    public TableId getChangeTableId() {
        return changeTableId;
    }

    public int getChangeTableObjectId() {
        return changeTableObjectId;
    }

    @Override
    public String toString() {
        return "ChangeTable{" +
                "captureInstance='" + captureInstance + '\'' +
                ", sourceTableId=" + sourceTableId +
                ", changeTableId=" + changeTableId +
                ", startLsn=" + startLsn +
                ", stopLsn=" + stopLsn +
                ", columnList=" + columnList +
                ", changeTableObjectId=" + changeTableObjectId +
                '}';
    }
}
