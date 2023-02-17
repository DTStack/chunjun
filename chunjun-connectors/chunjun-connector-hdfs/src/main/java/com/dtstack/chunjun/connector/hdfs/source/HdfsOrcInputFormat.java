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
package com.dtstack.chunjun.connector.hdfs.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.hdfs.InputSplit.HdfsOrcInputSplit;
import com.dtstack.chunjun.connector.hdfs.util.HdfsUtil;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.ReadRecordException;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
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

@Slf4j
public class HdfsOrcInputFormat extends BaseHdfsInputFormat {

    private static final String COMPLEX_FIELD_TYPE_SYMBOL_REGEX = ".*(<|>|\\{|}|[|]).*";
    private static final long serialVersionUID = 5825411463826640071L;
    private final AtomicBoolean isInit = new AtomicBoolean(false);
    private transient String[] fullColNames;
    private transient StructObjectInspector inspector;
    private transient List<? extends StructField> fields;

    @Override
    public HdfsOrcInputSplit[] createHdfsSplit(int minNumSplits) throws IOException {
        super.initHadoopJobConf();
        String path;
        if (StringUtils.isNotBlank(hdfsConfig.getFileName())) {
            // 兼容平台逻辑
            path =
                    hdfsConfig.getPath()
                            + ConstantValue.SINGLE_SLASH_SYMBOL
                            + hdfsConfig.getFileName();
        } else {
            path = hdfsConfig.getPath();
        }
        org.apache.hadoop.mapred.FileInputFormat.setInputPaths(hadoopJobConf, path);
        org.apache.hadoop.mapred.FileInputFormat.setInputPathFilter(
                hadoopJobConf, HdfsPathFilter.class);
        org.apache.hadoop.mapred.InputSplit[] splits =
                new OrcInputFormat().getSplits(hadoopJobConf, minNumSplits);

        if (splits != null) {
            List<HdfsOrcInputSplit> list = new ArrayList<>(splits.length);
            int i = 0;
            for (org.apache.hadoop.mapred.InputSplit split : splits) {
                OrcSplit orcSplit = (OrcSplit) split;
                // 49B file is empty
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
    public InputFormat createInputFormat() {
        return new OrcInputFormat();
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        HdfsOrcInputSplit hdfsOrcInputSplit = (HdfsOrcInputSplit) inputSplit;
        OrcSplit orcSplit = hdfsOrcInputSplit.getOrcSplit();

        if (openKerberos) {
            ugi.doAs(
                    (PrivilegedAction<Object>)
                            () -> {
                                try {
                                    init(orcSplit);
                                    openOrcReader(inputSplit);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                                return null;
                            });
        } else {
            init(orcSplit);
            openOrcReader(inputSplit);
        }
    }

    public void init(OrcSplit orcSplit) throws IOException {
        try {
            if (!isInit.get()) {
                init(orcSplit.getPath());
                isInit.set(true);
            }
        } catch (Exception e) {
            throw new IOException("init inspector error", e);
        }
    }

    private void openOrcReader(InputSplit inputSplit) throws IOException {
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

        try (org.apache.hadoop.hive.ql.io.orc.Reader reader =
                OrcFile.createReader(path, readerOptions)) {
            String typeStruct = reader.getObjectInspector().getTypeName();
            log.info("orc typeStruct = {}", typeStruct);

            if (StringUtils.isEmpty(typeStruct)) {
                throw new ChunJunRuntimeException("can't retrieve type struct from " + path);
            }

            int startIndex = typeStruct.indexOf("<") + 1;
            int endIndex = typeStruct.lastIndexOf(">");
            typeStruct = typeStruct.substring(startIndex, endIndex);

            if (typeStruct.matches(COMPLEX_FIELD_TYPE_SYMBOL_REGEX)) {
                throw new ChunJunRuntimeException(
                        "Field types such as array, map, and struct are not supported.");
            }

            List<String> columnList = parseColumnAndType(typeStruct);

            fullColNames = new String[columnList.size()];
            String[] fullColTypes = new String[columnList.size()];

            for (int i = 0; i < columnList.size(); ++i) {
                String[] temp = columnList.get(i).split(ConstantValue.COLON_SYMBOL);
                fullColNames[i] = temp[0];
                fullColTypes[i] = temp[1];
            }

            Properties p = new Properties();
            p.setProperty("columns", StringUtils.join(fullColNames, ConstantValue.COMMA_SYMBOL));
            p.setProperty(
                    "columns.types", StringUtils.join(fullColTypes, ConstantValue.COLON_SYMBOL));

            OrcSerde orcSerde = new OrcSerde();
            orcSerde.initialize(hadoopJobConf, p);

            this.inspector = (StructObjectInspector) orcSerde.getObjectInspector();
        }
    }

    private List<String> parseColumnAndType(String typeStruct) {
        List<String> columnList = new ArrayList<>();
        List<String> splitList = Arrays.asList(typeStruct.split(ConstantValue.COMMA_SYMBOL));
        Iterator<String> it = splitList.iterator();
        while (it.hasNext()) {
            StringBuilder current = new StringBuilder(it.next());
            String currentStr = current.toString();
            boolean left = currentStr.contains(ConstantValue.LEFT_PARENTHESIS_SYMBOL);
            boolean right = currentStr.contains(ConstantValue.RIGHT_PARENTHESIS_SYMBOL);
            if (!left && !right) {
                columnList.add(currentStr);
                continue;
            }

            if (left && right) {
                columnList.add(currentStr);
                continue;
            }

            if (left) {
                while (it.hasNext()) {
                    String next = it.next();
                    current.append(ConstantValue.COMMA_SYMBOL).append(next);
                    if (next.contains(ConstantValue.RIGHT_PARENTHESIS_SYMBOL)) {
                        break;
                    }
                }
                columnList.add(current.toString());
            }
        }
        return columnList;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        List<FieldConfig> fieldConfList = hdfsConfig.getColumn();
        GenericRowData genericRowData;
        if (fieldConfList.size() == 1
                && ConstantValue.STAR_SYMBOL.equals(fieldConfList.get(0).getName())) {
            genericRowData = new GenericRowData(fullColNames.length);
            for (int i = 0; i < fullColNames.length; i++) {
                Object obj = inspector.getStructFieldData(value, fields.get(i));
                genericRowData.setField(i, HdfsUtil.getWritableValue(obj));
            }
        } else {
            genericRowData = new GenericRowData(fieldConfList.size());
            for (int i = 0; i < fieldConfList.size(); i++) {
                FieldConfig fieldConfig = fieldConfList.get(i);
                Object obj = null;
                if (fieldConfig.getValue() != null) {
                    obj = fieldConfig.getValue();
                } else if (fieldConfig.getIndex() != null
                        && fieldConfig.getIndex() < fullColNames.length) {
                    obj = inspector.getStructFieldData(value, fields.get(fieldConfig.getIndex()));
                }

                genericRowData.setField(i, HdfsUtil.getWritableValue(obj));
            }
        }
        try {
            return rowConverter.toInternal(genericRowData);
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
    }
}
