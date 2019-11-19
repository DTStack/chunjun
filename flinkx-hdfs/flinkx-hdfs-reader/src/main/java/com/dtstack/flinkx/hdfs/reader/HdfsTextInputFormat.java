/**
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

import com.dtstack.flinkx.hdfs.HdfsUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.FileSystemUtil;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;

/**
 * The subclass of HdfsInputFormat which handles text files
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HdfsTextInputFormat extends HdfsInputFormat {

    @Override
    protected void configureAnythingElse() {
        this.inputFormat = new TextInputFormat();
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        try {
            JobConf jobConf = FileSystemUtil.getJobConf(hadoopConfig, defaultFS);

            org.apache.hadoop.mapred.FileInputFormat.setInputPaths(jobConf, inputPath);
            TextInputFormat inputFormat = new TextInputFormat();
            jobConf.set("mapreduce.input.fileinputformat.input.dir.recursive","true");
            inputFormat.configure(jobConf);
            org.apache.hadoop.mapred.InputSplit[] splits = inputFormat.getSplits(jobConf, minNumSplits);

            if(splits != null) {
                HdfsInputSplit.TextInputSplit[] hdfsTextInputSplits = new HdfsInputSplit.TextInputSplit[splits.length];
                for (int i = 0; i < splits.length; ++i) {
                    hdfsTextInputSplits[i] = new HdfsInputSplit.TextInputSplit(splits[i], i);
                }
                return hdfsTextInputSplits;
            }

            return new InputSplit[0];
        } catch (Exception e) {
            LOG.error("Create input split error:", e);

            return createErrorInputSplit(e);
        }
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        checkIfCreateSplitFailed(inputSplit);

        HdfsInputSplit.TextInputSplit hdfsTextInputSplit = (HdfsInputSplit.TextInputSplit) inputSplit;
        org.apache.hadoop.mapred.InputSplit fileSplit = hdfsTextInputSplit.getTextSplit();
        recordReader = inputFormat.getRecordReader(fileSplit, conf, Reporter.NULL);
        key = new LongWritable();
        value = new Text();
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        String line = new String(((Text)value).getBytes(), 0, ((Text)value).getLength(), charsetName);
        String[] fields = line.split(delimiter);

        if (metaColumns.size() == 1 && "*".equals(metaColumns.get(0).getName())){
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
}