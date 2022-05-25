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

package com.dtstack.chunjun.connector.inceptor.source;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.inceptor.inputSplit.InceptorOrcInputSplit;
import com.dtstack.chunjun.connector.inceptor.util.InceptorUtil;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.FileSystemUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.serde2.SerDeException;
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

public class InceptorOrcInputFormat extends BaseInceptorFileInputFormat {

    protected transient FileSystem fs;
    private final AtomicBoolean isInit = new AtomicBoolean(false);
    private transient String[] fullColNames;

    private transient StructObjectInspector inspector;

    private transient List<? extends StructField> fields;

    @Override
    public InceptorOrcInputSplit[] createInceptorSplit(int minNumSplits) throws IOException {
        super.initHadoopJobConf();
        String path;
        if (org.apache.commons.lang3.StringUtils.isNotBlank(inceptorFileConf.getFileName())) {
            // 兼容平台逻辑
            path =
                    inceptorFileConf.getPath()
                            + ConstantValue.SINGLE_SLASH_SYMBOL
                            + inceptorFileConf.getFileName();
        } else {
            path = inceptorFileConf.getPath();
        }
        org.apache.hadoop.mapred.FileInputFormat.setInputPaths(jobConf, path);
        org.apache.hadoop.mapred.FileInputFormat.setInputPathFilter(
                jobConf, InceptorPathFilter.class);
        org.apache.hadoop.mapred.InputSplit[] splits =
                new OrcInputFormat().getSplits(jobConf, minNumSplits);

        if (splits != null) {
            List<InceptorOrcInputSplit> list = new ArrayList<>(splits.length);
            int i = 0;
            for (org.apache.hadoop.mapred.InputSplit split : splits) {
                OrcSplit orcSplit = (OrcSplit) split;
                // 49B file is empty
                if (orcSplit.getLength() > 49) {
                    list.add(new InceptorOrcInputSplit(orcSplit, i));
                    i++;
                }
            }
            return list.toArray(new InceptorOrcInputSplit[i]);
        }

        return null;
    }

    @Override
    public InputFormat createInputFormat() {
        return new OrcInputFormat();
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        try {
            fs =
                    FileSystemUtil.getFileSystem(
                            inceptorFileConf.getHadoopConfig(),
                            inceptorFileConf.getDefaultFs(),
                            getRuntimeContext().getDistributedCache());
        } catch (Exception e) {
            LOG.error(
                    "Get FileSystem error on openInputFormat() method, hadoopConfig = {}, Exception = {}",
                    inceptorFileConf.getHadoopConfig().toString(),
                    ExceptionUtil.getErrorMessage(e));
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        InceptorOrcInputSplit inceptorOrcInputSplit = (InceptorOrcInputSplit) inputSplit;
        OrcSplit orcSplit = inceptorOrcInputSplit.getOrcSplit();

        try {
            if (!isInit.get()) {
                init(orcSplit.getPath());
                isInit.set(true);
            }
        } catch (Exception e) {
            throw new IOException("init [inspector] error", e);
        }
        ugi.doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        if (inceptorFileConf.isTransaction()) {
                            try {
                                openAcidRecordReader(inputSplit);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        } else {
                            try {
                                openOrcReader(inputSplit);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }

                        return null;
                    }
                });
    }

    private void init(Path path) throws Exception {
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(jobConf);
        String typeStruct =
                ugi.doAs(
                        new PrivilegedAction<String>() {
                            @Override
                            public String run() {
                                try {
                                    org.apache.hadoop.hive.ql.io.orc.Reader reader =
                                            OrcFile.createReader(path, readerOptions);
                                    return reader.getObjectInspector().getTypeName();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
        if (StringUtils.isEmpty(typeStruct)) {
            throw new RuntimeException("can't retrieve type struct from " + path);
        }
        LOG.info("original typeStruct {}", typeStruct);

        int startIndex = typeStruct.indexOf("<") + 1;
        int endIndex = typeStruct.lastIndexOf(">");
        typeStruct = typeStruct.substring(startIndex, endIndex);

        List<String> cols = parseColumnAndType(typeStruct);

        fullColNames = new String[cols.size()];
        String[] fullColTypes = new String[cols.size()];

        for (int i = 0; i < cols.size(); ++i) {
            int index = cols.get(i).indexOf(":");
            if (index > -1) {
                fullColNames[i] = cols.get(i).substring(0, index);
                fullColTypes[i] = cols.get(i).substring(index + 1);
            } else {
                LOG.warn("typeStruct {} is not valid", typeStruct);
            }
        }

        Properties p = new Properties();
        p.setProperty("columns", StringUtils.join(fullColNames, ","));
        p.setProperty("columns.types", StringUtils.join(fullColTypes, ":"));

        OrcSerde orcSerde = new OrcSerde();
        orcSerde.initialize(jobConf, p);

        this.inspector = (StructObjectInspector) orcSerde.getObjectInspector();
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        List<FieldConf> fieldConfList = inceptorFileConf.getColumn();
        GenericRowData genericRowData;
        if (fieldConfList.size() == 1
                && ConstantValue.STAR_SYMBOL.equals(fieldConfList.get(0).getName())) {
            genericRowData = new GenericRowData(fullColNames.length);
            for (int i = 0; i < fullColNames.length; i++) {
                Object col = inspector.getStructFieldData(value, fields.get(i));
                genericRowData.setField(i, InceptorUtil.getWritableValue(col));
            }
        } else {
            genericRowData = new GenericRowData(fieldConfList.size());
            for (int i = 0; i < fieldConfList.size(); i++) {
                FieldConf metaColumn = fieldConfList.get(i);
                Object val = null;

                if (metaColumn.getValue() != null) {
                    val = metaColumn.getValue();
                } else if (metaColumn.getIndex() != null
                        && metaColumn.getIndex() != -1
                        && metaColumn.getIndex() < fullColNames.length) {
                    val = inspector.getStructFieldData(value, fields.get(metaColumn.getIndex()));
                    if (val == null && metaColumn.getValue() != null) {
                        val = metaColumn.getValue();
                    } else {
                        val = InceptorUtil.getWritableValue(val);
                    }
                }

                genericRowData.setField(i, val);
            }
        }

        try {
            return rowConverter.toInternal(genericRowData);
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
    }

    /**
     * parse column and type from orc type struct string
     *
     * @param typeStruct
     * @return
     */
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

            if (left && !right) {
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

    /**
     * hive 事务表创建 RecordReader
     *
     * @param inputSplit
     */
    private void openAcidRecordReader(InputSplit inputSplit) {
        numReadCounter = getRuntimeContext().getLongCounter("numRead");
        InceptorOrcInputSplit hdfsOrcInputSplit = (InceptorOrcInputSplit) inputSplit;
        OrcSplit orcSplit = null;
        try {
            orcSplit = hdfsOrcInputSplit.getOrcSplit();
        } catch (IOException e) {
            LOG.error(
                    "Get orc split error, hdfsOrcInputSplit = {}, Exception = {}",
                    hdfsOrcInputSplit,
                    ExceptionUtil.getErrorMessage(e));
        }
        Path path = null;
        if (orcSplit != null) {
            path = orcSplit.getPath();
            // 处理分区
            findCurrentPartition(path);
        }
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(jobConf);
        readerOptions.filesystem(fs);

        org.apache.hadoop.hive.ql.io.orc.Reader reader = null;
        try {
            reader = OrcFile.createReader(path, readerOptions);
        } catch (IOException e) {
            LOG.error(
                    "Create reader error, path = {}, Exception = {}",
                    path,
                    ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException("Create reader error.");
        }

        String typeStruct = reader.getObjectInspector().getTypeName();
        if (StringUtils.isEmpty(typeStruct)) {
            throw new RuntimeException("can't retrieve type struct from " + path);
        }
        LOG.info("original typeStruct {}", typeStruct);

        int startIndex = typeStruct.indexOf("<") + 1;
        int endIndex = typeStruct.lastIndexOf(">");
        typeStruct = typeStruct.substring(startIndex, endIndex);
        startIndex = typeStruct.indexOf("<") + 1;
        endIndex = typeStruct.lastIndexOf(">");
        // typeStruct is
        // "operation:int,originalTransaction:bigint,bucket:int,rowId:bigint,currentTransaction:bigint,row:struct<>"
        if (startIndex != endIndex) {
            LOG.info("before typeStruct {} ", typeStruct);
            typeStruct = typeStruct.substring(startIndex, endIndex);
            LOG.info("after typeStruct {} ", typeStruct);
        } else {
            LOG.warn("typeStruct {} ", typeStruct);
        }

        //        if (typeStruct.matches(COMPLEX_FIELD_TYPE_SYMBOL_REGEX)) {
        //            throw new RuntimeException(
        //                    "Field types such as array, map, and struct are not supported.");
        //        }

        List<String> cols = parseColumnAndType(typeStruct);

        fullColNames = new String[cols.size()];
        String[] fullColTypes = new String[cols.size()];

        for (int i = 0; i < cols.size(); ++i) {
            int index = cols.get(i).indexOf(":");
            if (index > -1) {
                fullColNames[i] = cols.get(i).substring(0, index);
                fullColTypes[i] = cols.get(i).substring(index + 1);
            } else {
                LOG.warn("typeStruct {} is not valid", typeStruct);
            }
        }

        final String names = StringUtils.join(fullColNames, ",");
        final String types = StringUtils.join(fullColTypes, ":");
        Properties p = new Properties();
        p.setProperty("columns", names);
        p.setProperty("columns.types", types);

        OrcSerde orcSerde = new OrcSerde();
        orcSerde.initialize(jobConf, p);

        try {
            this.inspector = (StructObjectInspector) orcSerde.getObjectInspector();
        } catch (SerDeException e) {
            throw new RuntimeException("hive transaction table inspector create failed.");
        }
        // 读 hive 事务表需要设置的属性
        //        jobConf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, names);
        jobConf.set("schema.evolution.columns", names);
        // int:bigint:string:float:double:struct<writeId:bigint,bucketId:int,rowId:bigint>
        //        jobConf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, types);
        jobConf.set("schema.evolution.columns.types", types);
        //        AcidUtils.setAcidOperationalProperties(jobConf, true);
        try {
            recordReader = inputFormat.getRecordReader(orcSplit, jobConf, Reporter.NULL);
        } catch (IOException e) {
            throw new RuntimeException("hive transaction table record reader creation failed.");
        }
        key = recordReader.createKey();
        value = recordReader.createValue();
        fields = inspector.getAllStructFieldRefs();
    }

    private void openOrcReader(InputSplit inputSplit) throws IOException {
        numReadCounter = getRuntimeContext().getLongCounter("numRead");
        InceptorOrcInputSplit hdfsOrcInputSplit = (InceptorOrcInputSplit) inputSplit;
        OrcSplit orcSplit = hdfsOrcInputSplit.getOrcSplit();
        findCurrentPartition(orcSplit.getPath());
        recordReader = inputFormat.getRecordReader(orcSplit, jobConf, Reporter.NULL);
        key = recordReader.createKey();
        value = recordReader.createValue();
        fields = inspector.getAllStructFieldRefs();
    }
}
