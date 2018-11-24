package com.dtstack.flinkx.carbondata.reader;


import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.flink.core.io.InputSplit;
import java.io.IOException;
import java.util.List;

public class CarbonFlinkInputSplit implements InputSplit {

    private int splitNumber;

    private List<CarbonInputSplit> carbonInputSplits;

    public CarbonFlinkInputSplit(List<CarbonInputSplit> carbonInputSplits, int splitNumber) throws IOException {
        this.splitNumber = splitNumber;
        this.carbonInputSplits = carbonInputSplits;
    }

    public List<CarbonInputSplit> getCarbonInputSplits() throws IOException {
        return carbonInputSplits;
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }

}
