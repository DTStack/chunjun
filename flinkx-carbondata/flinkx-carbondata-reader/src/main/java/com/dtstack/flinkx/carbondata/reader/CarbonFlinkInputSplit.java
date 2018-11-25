package com.dtstack.flinkx.carbondata.reader;


import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.flink.core.io.InputSplit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CarbonFlinkInputSplit implements InputSplit {

    private int splitNumber;

    private List<byte[]> rawSplits;

    public CarbonFlinkInputSplit(List<CarbonInputSplit> carbonInputSplits, int splitNumber) throws IOException {
        this.splitNumber = splitNumber;
        rawSplits = new ArrayList<>();
        rawSplits.addAll(carbonInputSplits.stream().map(this::carbonSplitToRawSplit).collect(Collectors.toList()));
    }

    public List<CarbonInputSplit> getCarbonInputSplits() throws IOException {
        List<CarbonInputSplit> carbonInputSplits = rawSplits.stream().map(this::rawSplitToCarbonSplit).collect(Collectors.toList());
        return carbonInputSplits;
    }

    private byte[] carbonSplitToRawSplit(CarbonInputSplit carbonInputSplit) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            carbonInputSplit.write(dos);
        }  catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                baos.close();
                dos.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return baos.toByteArray();
    }

    private CarbonInputSplit rawSplitToCarbonSplit(byte[] rawSplit) {
        ByteArrayInputStream bais = new ByteArrayInputStream(rawSplit);
        DataInputStream dis = new DataInputStream(bais);
        CarbonInputSplit carbonInputSplit = new CarbonInputSplit();
        try {
            carbonInputSplit.readFields(dis);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                bais.close();
                dis.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return carbonInputSplit;
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }

}
