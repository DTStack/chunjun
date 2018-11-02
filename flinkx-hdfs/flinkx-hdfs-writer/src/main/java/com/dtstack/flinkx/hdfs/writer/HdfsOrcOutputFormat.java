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


package com.dtstack.flinkx.hdfs.writer;

import com.dtstack.flinkx.common.ColumnType;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.hdfs.HdfsUtil;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * The subclass of HdfsOutputFormat writing orc files
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HdfsOrcOutputFormat extends HdfsOutputFormat {
    private RecordWriter recordWriter;
    private OrcSerde orcSerde;
    private StructObjectInspector inspector;
    private FileOutputFormat outputFormat;
    private JobConf jobConf;


    @Override
    protected void configInternal() {
        orcSerde = new OrcSerde();
        outputFormat = new org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat();
        jobConf = new JobConf(conf);

        if(compress != null && compress.length() != 0 && !compress.equalsIgnoreCase("NONE")) {
            if(compress.equalsIgnoreCase("SNAPPY")) {
                FileOutputFormat.setOutputCompressorClass(jobConf, SnappyCodec.class);
            } else {
                throw new IllegalArgumentException("Unsupported compress format: " + compress);
            }
        }

        List<ObjectInspector>  fullColTypeList = new ArrayList<>();

        for(String columnType : fullColumnTypes) {
            columnType = columnType.toUpperCase();
            if(columnType.startsWith("DECIMAL")) {
                columnType = "DECIMAL";
            }
            ColumnType type = ColumnType.valueOf(columnType);
            fullColTypeList.add(HdfsUtil.columnTypeToObjectInspetor(type));
        }
        this.inspector = ObjectInspectorFactory
                .getStandardStructObjectInspector(fullColumnNames, fullColTypeList);

    }

    @Override
    public void open() throws IOException {
        recordWriter = outputFormat.getRecordWriter(null, jobConf, tmpPath, Reporter.NULL);
    }

    @Override
    public void writeSingleRecordInternal(Row row) throws WriteRecordException {
        int i = 0;
        try {
            List<Object> recordList = new ArrayList<>();
            for (; i < fullColumnNames.size(); ++i) {
                int j = colIndices[i];
                if(j == -1) {
                    recordList.add(null);
                    continue;
                }

                Object column = row.getField(j);

                if (column == null) {
                    recordList.add(null);
                    continue;
                }

                ColumnType columnType = ColumnType.fromString(columnTypes.get(j));
                String rowData = column.toString();
                if(rowData == null || rowData.length() == 0){
                    recordList.add(null);
                } else {
                    switch (columnType) {
                        case TINYINT:
                            recordList.add(Byte.valueOf(rowData));
                            break;
                        case SMALLINT:
                            recordList.add(Short.valueOf(rowData));
                            break;
                        case INT:
                            recordList.add(Integer.valueOf(rowData));
                            break;
                        case BIGINT:
                            BigInteger data = new BigInteger(rowData);
                            if (data.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) > 0){
                                recordList.add(data);
                            } else {
                                recordList.add(Long.valueOf(rowData));
                            }
                            break;
                        case FLOAT:
                            recordList.add(Float.valueOf(rowData));
                            break;
                        case DOUBLE:
                            recordList.add(Double.valueOf(rowData));
                            break;
                        case DECIMAL:
                            HiveDecimal hiveDecimal = HiveDecimal.create(new BigDecimal(rowData));
                            HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(hiveDecimal);
                            recordList.add(hiveDecimalWritable);
                            break;
                        case STRING:
                        case VARCHAR:
                        case CHAR:
                            recordList.add(rowData);
                            break;
                        case BOOLEAN:
                            recordList.add(Boolean.valueOf(rowData));
                            break;
                        case DATE:
                            recordList.add(DateUtil.columnToDate(column));
                            break;
                        case TIMESTAMP:
                            recordList.add(DateUtil.columnToTimestamp(column));
                            break;
                        default:
                            throw new IllegalArgumentException();
                    }
                }
            }
            this.recordWriter.write(NullWritable.get(), this.orcSerde.serialize(recordList, this.inspector));
        } catch(Exception e) {
            if(i < row.getArity()) {
                throw new WriteRecordException(recordConvertDetailErrorMessage(i, row), e, i, row);
            }
            throw new WriteRecordException(e.getMessage(), e);
        }

    }

    @Override
    protected String recordConvertDetailErrorMessage(int pos, Row row) {
        return "\nHdfsOrcOutputFormat [" + jobName + "] writeRecord error: when converting field[" + fullColumnNames.get(pos) + "] in Row(" + row + ")";
    }

    @Override
    public void closeInternal() throws IOException {
        RecordWriter rw = this.recordWriter;
        if(rw != null) {
            rw.close(Reporter.NULL);
            this.recordWriter = null;
        }
    }

}
