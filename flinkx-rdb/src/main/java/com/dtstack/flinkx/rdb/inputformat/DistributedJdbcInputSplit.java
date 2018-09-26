package com.dtstack.flinkx.rdb.inputformat;

import com.dtstack.flinkx.rdb.DataSource;
import org.apache.flink.core.io.GenericInputSplit;

import java.util.ArrayList;
import java.util.List;

public class DistributedJdbcInputSplit extends GenericInputSplit {

    private List<DataSource> sourceList;

    public DistributedJdbcInputSplit(int partitionNumber, int totalNumberOfPartitions) {
        super(partitionNumber, totalNumberOfPartitions);
    }

    public void addSource(List<DataSource> sourceLeft){
        if (sourceList == null){
            this.sourceList = new ArrayList<>();
        }

        this.sourceList.addAll(sourceLeft);
    }

    public void addSource(DataSource source){
        if (sourceList == null){
            this.sourceList = new ArrayList<>();
        }

        this.sourceList.add(source);
    }

    public List<DataSource> getSourceList() {
        return sourceList;
    }

    public void setSourceList(List<DataSource> sourceList) {
        this.sourceList = sourceList;
    }
}
