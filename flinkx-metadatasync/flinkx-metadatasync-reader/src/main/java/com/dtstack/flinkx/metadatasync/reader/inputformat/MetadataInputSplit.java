package com.dtstack.flinkx.metadatasync.reader.inputformat;
import org.apache.flink.core.io.GenericInputSplit;

/**
 * @author : tiezhu
 * @date : 2020/3/6
 * @description :
 */
public class MetadataInputSplit extends GenericInputSplit {
    private String dbUrl;
    private String table;

    /**
     * Creates a generic input split with the given split number.
     *
     * @param partitionNumber         The number of the split's partition.
     * @param totalNumberOfPartitions The total number of the splits (partitions).
     */
    public MetadataInputSplit(int partitionNumber, int totalNumberOfPartitions, String dbUrl, String table) {
        super(partitionNumber, totalNumberOfPartitions);
        this.dbUrl = dbUrl;
        this.table = table;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public String toString() {
        return "MetadataInputSplit{" +
                "dbUrl='" + dbUrl + '\'' +
                ", table='" + table + '\'' +
                '}';
    }
}
