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
import org.apache.commons.lang.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.Reporter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * The subclass of HdfsInputFormat which handles orc files
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HdfsOrcInputFormat extends HdfsInputFormat {

    private transient OrcSerde orcSerde;

    private transient String[] fullColNames;

    private transient String[] fullColTypes;

    private transient StructObjectInspector inspector;

    private transient List<? extends StructField> fields;

    private static final String COMPLEX_FIELD_TYPE_SYMBOL_REGEX = ".*(<|>|\\{|}|[|]).*";

    @Override
    protected void configureAnythingElse() {
        orcSerde = new OrcSerde();
        inputFormat = new OrcInputFormat();
        org.apache.hadoop.hive.ql.io.orc.Reader reader = null;
        try {
            FileSystem fs = FileSystemUtil.getFileSystem(hadoopConfig, defaultFS, jobId, "reader");
            OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);
            readerOptions.filesystem(fs);

            Path path = new Path(inputPath);
            String typeStruct = null;

            if(fs.isDirectory(path)) {
                RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, true);
                while(iterator.hasNext()) {
                    FileStatus fileStatus = iterator.next();
                    if(fileStatus.isFile() && fileStatus.getLen() > 49) {
                        Path subPath = fileStatus.getPath();
                        reader = OrcFile.createReader(subPath, readerOptions);
                        typeStruct = reader.getObjectInspector().getTypeName();
                        if(StringUtils.isNotEmpty(typeStruct)) {
                            break;
                        }
                    }
                }

                if(reader == null) {
                    //throw new RuntimeException("orcfile dir is empty!");
                    LOG.error("orc file {} is empty!", inputPath);
                    isFileEmpty = true;
                    return;
                }

            } else {
                reader = OrcFile.createReader(path, readerOptions);
                typeStruct = reader.getObjectInspector().getTypeName();
            }

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
            fullColTypes = new String[cols.size()];

            for(int i = 0; i < cols.size(); ++i) {
                String[] temp = cols.get(i).split(":");
                fullColNames[i] = temp[0];
                fullColTypes[i] = temp[1];
            }

            for(int j = 0; j < metaColumns.size(); ++j) {
                MetaColumn metaColumn = metaColumns.get(j);
                metaColumn.setIndex(name2index(metaColumn.getName()));
            }

            Properties p = new Properties();
            p.setProperty("columns", StringUtils.join(fullColNames, ","));
            p.setProperty("columns.types", StringUtils.join(fullColTypes, ":"));
            orcSerde.initialize(conf, p);

            this.inspector = (StructObjectInspector) orcSerde.getObjectInspector();

        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> parseColumnAndType(String typeStruct){
        List<String> cols = new ArrayList<>();
        List<String> splits = Arrays.asList(typeStruct.split(","));
        Iterator<String> it = splits.iterator();
        while (it.hasNext()){
            String current = it.next();
            if(current.contains("(")){
                if(current.contains("(")){
                    String next = it.next();
                    cols.add(current + "," + next);
                }
            } else {
                cols.add(current);
            }
        }

        return cols;
    }

    @Override
    public HdfsOrcInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        org.apache.hadoop.mapred.FileInputFormat.setInputPaths(conf, inputPath);
        org.apache.hadoop.mapred.InputSplit[] splits = inputFormat.getSplits(conf, minNumSplits);

        if(splits != null) {
            HdfsOrcInputSplit[] hdfsOrcInputSplits = new HdfsOrcInputSplit[splits.length];
            for (int i = 0; i < splits.length; ++i) {
                hdfsOrcInputSplits[i] = new HdfsOrcInputSplit((OrcSplit) splits[i], i);
            }
            return hdfsOrcInputSplits;
        }

        return null;
    }


    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {

        if(isFileEmpty){
            return;
        }

        numReadCounter = getRuntimeContext().getLongCounter("numRead");
        HdfsOrcInputSplit hdfsOrcInputSplit = (HdfsOrcInputSplit) inputSplit;
        OrcSplit orcSplit = hdfsOrcInputSplit.getOrcSplit();
        recordReader = inputFormat.getRecordReader(orcSplit, conf, Reporter.NULL);
        key = recordReader.createKey();
        value = recordReader.createValue();
        fields = inspector.getAllStructFieldRefs();
    }

    private int name2index(String columnName) {
        int i = 0;
        for(; i < fullColNames.length; ++i) {
            if (fullColNames[i].equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }


    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        if(metaColumns.size() == 1 && "*".equals(metaColumns.get(0).getName())){
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

                if(metaColumn.getIndex() != -1){
                    val = inspector.getStructFieldData(value, fields.get(metaColumn.getIndex()));
                    if (val == null && metaColumn.getValue() != null){
                        val = metaColumn.getValue();
                    }
                } else if(metaColumn.getValue() != null){
                    val = metaColumn.getValue();
                }

                if(val instanceof String || val instanceof org.apache.hadoop.io.Text){
                    val = HdfsUtil.string2col(String.valueOf(val),metaColumn.getType(),metaColumn.getTimeFormat());
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
