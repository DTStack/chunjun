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

package com.dtstack.chunjun.connector.inceptor.inputSplit;

import org.apache.flink.core.io.InputSplit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class InceptorTextInputSplit implements InputSplit {
    int splitNumber;
    byte[] textSplitData;

    public InceptorTextInputSplit(org.apache.hadoop.mapred.InputSplit split, int splitNumber)
            throws IOException {
        this.splitNumber = splitNumber;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        split.write(dos);
        textSplitData = baos.toByteArray();
        baos.close();
        dos.close();
    }

    public org.apache.hadoop.mapred.InputSplit getTextSplit() throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(textSplitData);
        DataInputStream dis = new DataInputStream(bais);
        org.apache.hadoop.mapred.InputSplit split =
                new FileSplit((Path) null, 0L, 0L, (String[]) null);
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
