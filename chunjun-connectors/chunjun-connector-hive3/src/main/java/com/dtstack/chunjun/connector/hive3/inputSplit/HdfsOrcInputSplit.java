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

package com.dtstack.chunjun.connector.hive3.inputSplit;

import org.apache.flink.core.io.InputSplit;

import org.apache.hadoop.hive.ql.io.orc.OrcSplit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class HdfsOrcInputSplit implements InputSplit {
    private static final long serialVersionUID = -956858589799307007L;

    int splitNumber;
    byte[] orcSplitData;

    public HdfsOrcInputSplit(OrcSplit orcSplit, int splitNumber) throws IOException {
        this.splitNumber = splitNumber;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        orcSplit.write(dos);
        orcSplitData = baos.toByteArray();
        baos.close();
        dos.close();
    }

    public OrcSplit getOrcSplit() throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(orcSplitData);
        DataInputStream dis = new DataInputStream(bais);
        // TODO 修改 分片代码
        OrcSplit orcSplit =
                new OrcSplit(
                        null, null, 0, 0, null, null, false, false, new ArrayList(), 0, 0, null);
        orcSplit.readFields(dis);
        bais.close();
        dis.close();
        return orcSplit;
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }
}
