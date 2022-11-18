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

package com.dtstack.chunjun.connector.hive3.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.hive3.inputSplit.HdfsOrcInputSplit;
import com.dtstack.chunjun.connector.hive3.util.Hive3Util;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.ReadRecordException;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class HdfsOrcInputFormat extends BaseHdfsInputFormat {
    private static final long serialVersionUID = 5344370628109946753L;

    protected transient FileSystem fs;

    protected transient String[] fullColNames;

    protected transient StructObjectInspector inspector;

    protected transient List<? extends StructField> fields;

    protected static final String COMPLEX_FIELD_TYPE_SYMBOL_REGEX = ".*(<|>|\\{|}|[|]).*";

    private final AtomicBoolean isInit = new AtomicBoolean(false);

    @Override
    protected InputSplit[] createHdfsSplit(int minNumSplits) throws IOException {
        initHadoopJobConf();
        // 非事务表创建分片
        org.apache.hadoop.mapred.FileInputFormat.setInputPaths(hadoopJobConf, hdfsConfig.getPath());
        org.apache.hadoop.mapred.FileInputFormat.setInputPathFilter(
                hadoopJobConf, HdfsPathFilter.class);

        OrcInputFormat orcInputFormat = new OrcInputFormat();
        org.apache.hadoop.mapred.InputSplit[] splits =
                orcInputFormat.getSplits(hadoopJobConf, minNumSplits);

        if (splits != null) {
            List<HdfsOrcInputSplit> list = new ArrayList<>(splits.length);
            int i = 0;
            for (org.apache.hadoop.mapred.InputSplit split : splits) {
                OrcSplit orcSplit = (OrcSplit) split;
                if (orcSplit.getLength() > 49) {
                    list.add(new HdfsOrcInputSplit(orcSplit, i));
                    i++;
                }
            }
            return list.toArray(new HdfsOrcInputSplit[i]);
        }
        return null;
    }

    @Override
    public InputFormat<NullWritable, OrcStruct> createMapredInputFormat() {
        return new OrcInputFormat();
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        if (super.openKerberos) {
            ugi.doAs(
                    (PrivilegedAction<Object>)
                            () -> {
                                try {
                                    orcOpenInternal(inputSplit);
                                } catch (IOException e) {
                                    throw new ChunJunRuntimeException(
                                            "failed to open orc internal", e);
                                }
                                return null;
                            });
        } else {
            orcOpenInternal(inputSplit);
        }
    }

    protected void orcOpenInternal(InputSplit inputSplit) throws IOException {
        OrcSplit orcSplit = ((HdfsOrcInputSplit) inputSplit).getOrcSplit();
        fs = FileSystem.get(hadoopJobConf);
        init(orcSplit);
        openOrcReader(inputSplit);
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        try {
            List<FieldConfig> fieldConfList = hdfsConfig.getColumn();
            GenericRowData genericRowData =
                    new GenericRowData(Math.max(fieldConfList.size(), fullColNames.length));
            if (fieldConfList.size() == 1
                    && ConstantValue.STAR_SYMBOL.equals(fieldConfList.get(0).getName())) {

                for (int i = 0; i < fullColNames.length; i++) {
                    Object col = inspector.getStructFieldData(value, fields.get(i));
                    if (col != null) {
                        col = Hive3Util.getWritableValue(col);
                    }
                    genericRowData.setField(i, col);
                }
            } else {
                for (int i = 0; i < fieldConfList.size(); i++) {
                    FieldConfig fieldConfig = fieldConfList.get(i);
                    Object val = null;
                    if (fieldConfig.getValue() != null) {
                        val = fieldConfig.getValue();
                    } else if (fieldConfig.getIndex() != -1) {
                        val =
                                inspector.getStructFieldData(
                                        value, fields.get(fieldConfig.getIndex()));
                    }

                    if (val != null) {
                        val = Hive3Util.getWritableValue(val);
                    }

                    genericRowData.setField(i, val);
                }
            }
            return rowConverter.toInternal(genericRowData);
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
    }

    protected List<String> parseColumnAndType(String typeStruct) {
        List<String> cols = new ArrayList<>();
        List<String> splits = Arrays.asList(typeStruct.split(","));
        Iterator<String> it = splits.iterator();
        while (it.hasNext()) {
            StringBuilder current = new StringBuilder(it.next());
            if (!current.toString().contains("(") && !current.toString().contains(")")) {
                cols.add(current.toString());
                continue;
            }

            if (current.toString().contains("(") && current.toString().contains(")")) {
                cols.add(current.toString());
                continue;
            }

            if (current.toString().contains("(") && !current.toString().contains(")")) {
                while (it.hasNext()) {
                    String next = it.next();
                    current.append(",").append(next);
                    if (next.contains(")")) {
                        break;
                    }
                }

                cols.add(current.toString());
            }
        }

        return cols;
    }

    public void init(OrcSplit orcSplit) throws IOException {
        try {
            if (!isInit.get()) {
                init(orcSplit.getPath());
                isInit.set(true);
            }
        } catch (Exception e) {
            throw new IOException("init [inspector] error", e);
        }
    }

    private void openOrcReader(InputSplit inputSplit) throws IOException {
        numReadCounter = getRuntimeContext().getLongCounter("numRead");
        HdfsOrcInputSplit hdfsOrcInputSplit = (HdfsOrcInputSplit) inputSplit;
        OrcSplit orcSplit = hdfsOrcInputSplit.getOrcSplit();
        findCurrentPartition(orcSplit.getPath());
        recordReader = inputFormat.getRecordReader(orcSplit, hadoopJobConf, Reporter.NULL);
        key = recordReader.createKey();
        value = recordReader.createValue();
        fields = inspector.getAllStructFieldRefs();
    }

    private void init(Path path) throws Exception {
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(hadoopJobConf);
        readerOptions.filesystem(fs);

        org.apache.hadoop.hive.ql.io.orc.Reader reader = OrcFile.createReader(path, readerOptions);
        String typeStruct = reader.getObjectInspector().getTypeName();
        // struct<id:int,name:string,age:int>
        if (StringUtils.isEmpty(typeStruct)) {
            throw new RuntimeException("can't retrieve type struct from " + path);
        }

        int startIndex = typeStruct.indexOf("<") + 1;
        int endIndex = typeStruct.lastIndexOf(">");
        typeStruct = typeStruct.substring(startIndex, endIndex);

        if (typeStruct.matches(COMPLEX_FIELD_TYPE_SYMBOL_REGEX)) {
            throw new RuntimeException(
                    "Field types such as array, map, and struct are not supported.");
        }

        List<String> cols = parseColumnAndType(typeStruct);

        fullColNames = new String[cols.size()];
        String[] fullColTypes = new String[cols.size()];

        for (int i = 0; i < cols.size(); ++i) {
            String[] temp = cols.get(i).split(":");
            fullColNames[i] = temp[0];
            fullColTypes[i] = temp[1];
        }

        Properties p = new Properties();
        p.setProperty("columns", StringUtils.join(fullColNames, ","));
        p.setProperty("columns.types", StringUtils.join(fullColTypes, ":"));

        OrcSerde orcSerde = new OrcSerde();
        orcSerde.initialize(hadoopJobConf, p);

        this.inspector = (StructObjectInspector) orcSerde.getObjectInspector();
    }
}
