package com.dtstack.flinkx.metadata.reader.inputformat;

import org.apache.flink.core.io.GenericInputSplit;

/**
 * @author : tiezhu
 * @date : 2020/3/8
 * @description :
 */
public class MetaDataInputSplit extends GenericInputSplit {
    private String dbUrl;
    private String table;


    /**
     * Creates a generic input split with the given split number.
     *
     * @param partitionNumber         The number of the split's partition.
     * @param totalNumberOfPartitions The total number of the splits (partitions).
     */
    public MetaDataInputSplit(int partitionNumber, int totalNumberOfPartitions, String dbUrl, String table) {
        super(partitionNumber, totalNumberOfPartitions);
        this.dbUrl = dbUrl;
        this.table = table;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public String getTable() {
        return table;
    }


    @Override
    public String toString() {
        return "MetaDataInputSplit{" +
                "dbUrl='" + dbUrl + '\'' +
                ", table='" + table + '\'' +
                '}';
    }
}
