/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.hdfs.InputSplit;

import org.apache.flink.core.io.InputSplit;

import org.apache.hadoop.mapred.FileSplit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class HdfsTextInputSplit implements InputSplit {
    private static final long serialVersionUID = -5705728781573308377L;

    int splitNumber;
    byte[] textSplitData;

    public HdfsTextInputSplit(org.apache.hadoop.mapred.InputSplit split, int splitNumber)
            throws IOException {
        this.splitNumber = splitNumber;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(stream);
        split.write(dos);
        textSplitData = stream.toByteArray();
        stream.close();
        dos.close();
    }

    public org.apache.hadoop.mapred.InputSplit getTextSplit() throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(textSplitData);
        DataInputStream dis = new DataInputStream(bais);
        org.apache.hadoop.mapred.InputSplit split = new FileSplit(null, 0L, 0L, (String[]) null);
        split.readFields(dis);
        bais.close();
        dis.close();
        return split;
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }
}
