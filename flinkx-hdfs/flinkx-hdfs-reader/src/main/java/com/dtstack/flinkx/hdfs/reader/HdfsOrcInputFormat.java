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
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The subclass of HdfsInputFormat which handles orc files
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HdfsOrcInputFormat extends BaseHdfsInputFormat {

    private transient String[] fullColNames;

    private transient StructObjectInspector inspector;

    private transient List<? extends StructField> fields;

    private static final String COMPLEX_FIELD_TYPE_SYMBOL_REGEX = ".*(<|>|\\{|}|[|]).*";

    private AtomicBoolean isInit = new AtomicBoolean(false);

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        inputFormat = new OrcInputFormat();
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        HdfsOrcInputSplit hdfsOrcInputSplit = (HdfsOrcInputSplit) inputSplit;
        OrcSplit orcSplit = hdfsOrcInputSplit.getOrcSplit();

        try {
            if (!isInit.get()) {
                init(orcSplit.getPath());
                isInit.set(true);
            }
        } catch (Exception e) {
            throw new IOException("初始化[inspector]出错", e);
        }

        if (openKerberos) {
            ugi.doAs(new PrivilegedAction<Object>() {
                @Override
                public Object run() {
                    try {
                        openOrcReader(inputSplit);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    return null;
                }
            });
        } else {
            openOrcReader(inputSplit);
        }
    }

    private void openOrcReader(InputSplit inputSplit) throws IOException{
        numReadCounter = getRuntimeContext().getLongCounter("numRead");
        HdfsOrcInputSplit hdfsOrcInputSplit = (HdfsOrcInputSplit) inputSplit;
        OrcSplit orcSplit = hdfsOrcInputSplit.getOrcSplit();
        findCurrentPartition(orcSplit.getPath());
        recordReader = inputFormat.getRecordReader(orcSplit, conf, Reporter.NULL);
        key = recordReader.createKey();
        value = recordReader.createValue();
        fields = inspector.getAllStructFieldRefs();
    }

    private void init(Path path) throws Exception {
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);
        readerOptions.filesystem(fs);

        org.apache.hadoop.hive.ql.io.orc.Reader reader = OrcFile.createReader(path, readerOptions);
        String typeStruct = reader.getObjectInspector().getTypeName();

        if (StringUtils.isEmpty(typeStruct)) {
            throw new RuntimeException("can't retrieve type struct from " + path);
        }

        int startIndex = typeStruct.indexOf("<") + 1;
        int endIndex = typeStruct.lastIndexOf(">");
        typeStruct = typeStruct.substring(startIndex, endIndex);

        if(typeStruct.matches(COMPLEX_FIELD_TYPE_SYMBOL_REGEX)){
            throw new RuntimeException("Field types such as array, map, and struct are not supported.");
        }

        List<String> cols = parseColumnAndType(typeStruct);

        fullColNames = new String[cols.size()];
        String[] fullColTypes = new String[cols.size()];

        for(int i = 0; i < cols.size(); ++i) {
            String[] temp = cols.get(i).split(":");
            fullColNames[i] = temp[0];
            fullColTypes[i] = temp[1];
        }

        Properties p = new Properties();
        p.setProperty("columns", StringUtils.join(fullColNames, ","));
        p.setProperty("columns.types", StringUtils.join(fullColTypes, ":"));

        OrcSerde orcSerde = new OrcSerde();
        orcSerde.initialize(conf, p);

        this.inspector = (StructObjectInspector) orcSerde.getObjectInspector();
    }

    private List<String> parseColumnAndType(String typeStruct){
        List<String> cols = new ArrayList<>();
        List<String> splits = Arrays.asList(typeStruct.split(","));
        Iterator<String> it = splits.iterator();
        while (it.hasNext()){
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

    @Override
    public HdfsOrcInputSplit[] createInputSplitsInternal(int minNumSplits) throws IOException {
        if (FileSystemUtil.isOpenKerberos(hadoopConfig)) {
            UserGroupInformation ugi = FileSystemUtil.getUGI(hadoopConfig, defaultFs);
            LOG.info("user:{}, ", ugi.getShortUserName());
            return ugi.doAs(new PrivilegedAction<HdfsOrcInputSplit[]>() {
                @Override
                public HdfsOrcInputSplit[] run() {
                    try {
                        return createOrcSplit(minNumSplits);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        } else {
            return createOrcSplit(minNumSplits);
        }
    }

    private HdfsOrcInputSplit[] createOrcSplit(int minNumSplits) throws IOException{
        JobConf jobConf = FileSystemUtil.getJobConf(hadoopConfig, defaultFs);
        org.apache.hadoop.mapred.FileInputFormat.setInputPaths(jobConf, inputPath);
        org.apache.hadoop.mapred.FileInputFormat.setInputPathFilter(buildConfig(), HdfsPathFilter.class);

        OrcInputFormat orcInputFormat = new OrcInputFormat();
        org.apache.hadoop.mapred.InputSplit[] splits = orcInputFormat.getSplits(jobConf, minNumSplits);

        if(splits != null) {
            List<HdfsOrcInputSplit> list = new ArrayList<>(splits.length);
            int i = 0;
            for (org.apache.hadoop.mapred.InputSplit split : splits) {
                OrcSplit orcSplit = (OrcSplit) split;
                if(orcSplit.getLength() > 49){
                    list.add(new HdfsOrcInputSplit(orcSplit, i));
                    i++;
                }
            }
            return list.toArray(new HdfsOrcInputSplit[i]);
        }

        return null;
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        if(metaColumns.size() == 1 && ConstantValue.STAR_SYMBOL.equals(metaColumns.get(0).getName())){
            row = new Row(fullColNames.length);
            for (int i = 0; i < fullColNames.length; i++) {
                Object col = inspector.getStructFieldData(value, fields.get(i));
                if (col != null) {
                    col = HdfsUtil.getWritableValue(col);
                }
                row.setField(i, col);
            }
        } else {
            row = new Row(metaColumns.size());
            for (int i = 0; i < metaColumns.size(); i++) {
                MetaColumn metaColumn = metaColumns.get(i);
                Object val = null;

                if(metaColumn.getValue() != null){
                    val = metaColumn.getValue();
                }else if(metaColumn.getIndex() != -1){
                    val = inspector.getStructFieldData(value, fields.get(metaColumn.getIndex()));
                    if (val == null && metaColumn.getValue() != null){
                        val = metaColumn.getValue();
                    }
                }

                if(val instanceof String || val instanceof org.apache.hadoop.io.Text){
                    val = StringUtil.string2col(String.valueOf(val), metaColumn.getType(), metaColumn.getTimeFormat());
                } else if(val != null){
                    val = HdfsUtil.getWritableValue(val);
                }

                row.setField(i,val);
            }
        }

        return row;
    }

    static class HdfsOrcInputSplit implements InputSplit {
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
}
