/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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


/**
 * Carbondata Flink Split
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbonFlinkInputSplit implements InputSplit {

    private int splitNumber;

    private List<byte[]> rawSplits;

    public CarbonFlinkInputSplit(List<CarbonInputSplit> carbonInputSplits, int splitNumber) throws IOException {
        this.splitNumber = splitNumber;
        rawSplits = new ArrayList<>();
        List<byte[]> list = new ArrayList<>();
        for (CarbonInputSplit carbonInputSplit : carbonInputSplits) {
            byte[] bytes = carbonSplitToRawSplit(carbonInputSplit);
            list.add(bytes);
        }
        rawSplits.addAll(list);
    }

    public List<CarbonInputSplit> getCarbonInputSplits() throws IOException {
        List<CarbonInputSplit> carbonInputSplits = new ArrayList<>();
        for (byte[] rawSplit : rawSplits) {
            CarbonInputSplit carbonInputSplit = rawSplitToCarbonSplit(rawSplit);
            carbonInputSplits.add(carbonInputSplit);
        }
        return carbonInputSplits;
    }

    private byte[] carbonSplitToRawSplit(CarbonInputSplit carbonInputSplit) throws IOException{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            carbonInputSplit.write(dos);
        }  catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            baos.close();
            dos.close();
        }

        return baos.toByteArray();
    }

    private CarbonInputSplit rawSplitToCarbonSplit(byte[] rawSplit) throws IOException{
        ByteArrayInputStream bais = new ByteArrayInputStream(rawSplit);
        DataInputStream dis = new DataInputStream(bais);
        CarbonInputSplit carbonInputSplit = new CarbonInputSplit();
        try {
            carbonInputSplit.readFields(dis);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            bais.close();
            dis.close();
        }

        return carbonInputSplit;
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }

}
