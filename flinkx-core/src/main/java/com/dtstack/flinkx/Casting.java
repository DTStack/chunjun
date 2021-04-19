package com.dtstack.flinkx;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;

@FunctionalInterface
public interface Casting extends Serializable{

    Object apply(RowData rowData, int pos);

}
