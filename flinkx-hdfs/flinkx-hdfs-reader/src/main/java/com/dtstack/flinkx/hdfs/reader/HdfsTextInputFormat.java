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

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.hdfs.HdfsUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.FileSystemUtil;
import jodd.util.StringUtil;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileSplit;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Map;

/**
 * The subclass of HdfsInputFormat which handles text files
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HdfsTextInputFormat extends BaseHdfsInputFormat {

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        this.inputFormat = new TextInputFormat();
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws IOException {
        try {
            FileSystemUtil.getFileSystem(hadoopConfig, defaultFs);
        } catch (Exception e) {
            throw new IOException(e);
        }

        JobConf jobConf = buildConfig();
        org.apache.hadoop.mapred.FileInputFormat.setInputPathFilter(jobConf, HdfsPathFilter.class);

        org.apache.hadoop.mapred.FileInputFormat.setInputPaths(jobConf, inputPath);
        TextInputFormat inputFormat = new TextInputFormat();

        jobConf.set("mapreduce.input.fileinputformat.input.dir.recursive","true");
        inputFormat.configure(jobConf);
        org.apache.hadoop.mapred.InputSplit[] splits = inputFormat.getSplits(jobConf, minNumSplits);

        if(splits != null) {
            HdfsTextInputSplit[] hdfsTextInputSplits = new HdfsTextInputSplit[splits.length];
            for (int i = 0; i < splits.length; ++i) {
                hdfsTextInputSplits[i] = new HdfsTextInputSplit(splits[i], i);
            }
            return hdfsTextInputSplits;
        }

        return null;
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        HdfsTextInputSplit hdfsTextInputSplit = (HdfsTextInputSplit) inputSplit;
        org.apache.hadoop.mapred.InputSplit fileSplit = hdfsTextInputSplit.getTextSplit();
        recordReader = inputFormat.getRecordReader(fileSplit, conf, Reporter.NULL);
        key = new LongWritable();
        value = new Text();
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        String line = new String(((Text)value).getBytes(), 0, ((Text)value).getLength(), charsetName);
        String[] fields = line.split(delimiter);

        if (metaColumns.size() == 1 && ConstantValue.STAR_SYMBOL.equals(metaColumns.get(0).getName())){
            row = new Row(fields.length);
            for (int i = 0; i < fields.length; i++) {
                row.setField(i, fields[i]);
            }
        } else {
            row = new Row(metaColumns.size());
            for (int i = 0; i < metaColumns.size(); i++) {
                MetaColumn metaColumn = metaColumns.get(i);

                Object value = null;
                if(metaColumn.getValue() != null){
                    value = metaColumn.getValue();
                } else if(metaColumn.getIndex() != null && metaColumn.getIndex() < fields.length){
                    String strVal = fields[metaColumn.getIndex()];
                    if (!HdfsUtil.NULL_VALUE.equals(strVal)){
                        value = strVal;
                    }
                }

                if(value != null){
                    value = HdfsUtil.string2col(String.valueOf(value),metaColumn.getType(),metaColumn.getTimeFormat());
                }

                row.setField(i, value);
            }
        }

        return row;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        key = new LongWritable();
        value = new Text();
        return isFileEmpty || !recordReader.next(key, value);
    }


    public static class HdfsTextInputFormatBuilder {

        private HdfsTextInputFormat format;

        private HdfsTextInputFormatBuilder() {
            format = new HdfsTextInputFormat();
        }

        public HdfsTextInputFormatBuilder setHadoopConfig(Map<String,Object> hadoopConfig) {
            format.hadoopConfig = hadoopConfig;
            return this;
        }

        public HdfsTextInputFormatBuilder setInputPaths(String inputPaths) {
            format.inputPath = inputPaths;
            return this;
        }

        public HdfsTextInputFormatBuilder setBytes(long bytes) {
            format.bytes = bytes;
            return this;
        }

        public HdfsTextInputFormatBuilder setMonitorUrls(String monitorUrls) {
            format.monitorUrls = monitorUrls;
            return this;
        }

        public HdfsTextInputFormatBuilder setDelimiter(String delimiter) {
            if(delimiter == null) {
                delimiter = "\\001";
            }
            format.delimiter = delimiter;
            return this;
        }

        public HdfsTextInputFormatBuilder setDefaultFs(String defaultFs) {
            format.defaultFs = defaultFs;
            return this;
        }

        public HdfsTextInputFormatBuilder setcharsetName (String charsetName) {
            if(StringUtil.isNotEmpty(charsetName)) {
                if(!Charset.isSupported(charsetName)) {
                    throw new UnsupportedCharsetException("The charset " + charsetName + " is not supported.");
                }
                this.format.charsetName = charsetName;
            }

            return this;
        }

        public HdfsTextInputFormat finish() {
            return format;
        }
    }

    public static HdfsTextInputFormatBuilder buildHdfsTextInputFormat() {
        return new HdfsTextInputFormatBuilder();
    }

    static class HdfsTextInputSplit implements InputSplit {
        int splitNumber;
        byte[] textSplitData;

        public HdfsTextInputSplit(org.apache.hadoop.mapred.InputSplit split, int splitNumber) throws IOException {
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

}