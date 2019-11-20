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


package com.dtstack.flinkx.hdfs.reader;

import org.apache.flink.core.io.InputSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.mapred.FileSplit;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jiangbo
 * @date 2019/11/19
 */
public class HdfsInputSplit {

    static class ErrorInputSplit implements InputSplit {

        int splitNumber;

        String errorMessage;

        public ErrorInputSplit(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public String getErrorMessage(){
            return errorMessage;
        }

        @Override
        public int getSplitNumber() {
            return splitNumber;
        }
    }

    static class TextInputSplit implements InputSplit {
        int splitNumber;
        byte[] textSplitData;

        public TextInputSplit(org.apache.hadoop.mapred.InputSplit split, int splitNumber) throws IOException {
            this.splitNumber = splitNumber;
            org.apache.commons.io.output.ByteArrayOutputStream baos = new org.apache.commons.io.output.ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            split.write(dos);
            textSplitData = baos.toByteArray();
            baos.close();
            dos.close();
        }

        public org.apache.hadoop.mapred.InputSplit getTextSplit() throws IOException {
            ByteArrayInputStream bais = new ByteArrayInputStream(textSplitData);
            DataInputStream dis = new DataInputStream(bais);
            org.apache.hadoop.mapred.InputSplit split = new FileSplit((Path)null, 0L, 0L, (String[])null);
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

    static class OrcInputSplit implements InputSplit {
        int splitNumber;
        byte[] orcSplitData;

        public OrcInputSplit(OrcSplit orcSplit, int splitNumber) throws IOException {
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
            OrcSplit orcSplit = new OrcSplit(null, 0, 0, null, null
                    , false, false,new ArrayList());
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

    static class ParquetInputSplit implements InputSplit{
        int splitNumber;
        private List<String> paths;

        public ParquetInputSplit(int splitNumber, List<String> paths) {
            this.splitNumber = splitNumber;
            this.paths = paths;
        }

        public List<String> getPaths() {
            return paths;
        }

        @Override
        public int getSplitNumber() {
            return splitNumber;
        }
    }
}
