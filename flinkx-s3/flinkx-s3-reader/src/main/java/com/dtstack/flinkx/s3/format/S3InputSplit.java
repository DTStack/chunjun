package com.dtstack.flinkx.s3.format;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.core.io.GenericInputSplit;

import java.util.*;

public class S3InputSplit extends GenericInputSplit {

    private List<String> splits;



    public S3InputSplit(int partitionNumber, int totalNumberOfPartitions) {
        super(partitionNumber, totalNumberOfPartitions);
        this.splits = new ArrayList<>();
    }

    /**
     * Creates a generic input split with the given split number.
     *
     * @param partitionNumber         The number of the split's partition.
     * @param totalNumberOfPartitions The total number of the splits (partitions).
     */
    public S3InputSplit(int partitionNumber, int totalNumberOfPartitions, List<String> splits) {
        super(partitionNumber, totalNumberOfPartitions);
        this.splits = splits;
    }

    public List<String> getSplits() {
        return splits;
    }

    public void setSplits(List<String> splits) {
        this.splits = splits;
    }

    public void addSplit(String split){
        splits.add(split);
    }
}
