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
import com.dtstack.flinkx.util.DateUtil;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * The builder class of HdfsOutputFormat writing text files
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HdfsTextOutputFormat extends HdfsOutputFormat {

    private static final int NEWLINE = 10;
    protected transient OutputStream stream;
    private long nLine = 0;

    @Override
    public void open() throws IOException {
        Path p  = new Path(tmpPath);
        if(compress == null || compress.length() == 0) {
            stream = fs.create(p);
        } else if (compress.equalsIgnoreCase("GZIP")) {
            tmpPath = tmpPath + ".gz";
            p = new Path(tmpPath);
            stream = new GzipCompressorOutputStream(fs.create(p));
        } else if (compress.equalsIgnoreCase("BZIP2")) {
            tmpPath = tmpPath + ".bz2";
            p = new Path(tmpPath);
            stream = new BZip2CompressorOutputStream(fs.create(p));
        } else {
            throw new IllegalArgumentException("Unsupported compress type: " + compress);
        }
    }

    @Override
    public void writeSingleRecordInternal(Row row) throws WriteRecordException {
        // write string to hdfs
        byte[] bytes = null;
        int i = 0;
        try {
            // convert row to string
            int cnt = fullColumnNames.size();
            StringBuilder sb = new StringBuilder();

            for (; i < cnt; ++i) {
                if (i != 0) {
                    sb.append(delimiter);
                }

                int j = colIndices[i];
                if(j == -1) {
                    continue;
                }

                Object column = row.getField(j);

                if(column == null) {
                    continue;
                }

                String rowData = column.toString();
                ColumnType columnType = ColumnType.fromString(columnTypes.get(j));

                if(rowData == null || rowData.length() == 0){
                    sb.append("");
                } else {
                    switch (columnType) {
                        case TINYINT:
                            sb.append(Byte.valueOf(rowData));
                            break;
                        case SMALLINT:
                            sb.append(Short.valueOf(rowData));
                            break;
                        case INT:
                            sb.append(Integer.valueOf(rowData));
                            break;
                        case BIGINT:
                            BigInteger data = new BigInteger(rowData);
                            if (data.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) > 0){
                                sb.append(data);
                            } else {
                                sb.append(Long.valueOf(rowData));
                            }
                            break;
                        case FLOAT:
                            sb.append(Float.valueOf(rowData));
                            break;
                        case DOUBLE:
                            sb.append(Double.valueOf(rowData));
                            break;
                        case DECIMAL:
                            sb.append(HiveDecimal.create(new BigDecimal(rowData)));
                            break;
                        case STRING:
                        case VARCHAR:
                        case CHAR:
                            sb.append(rowData);
                            break;
                        case BOOLEAN:
                            sb.append(Boolean.valueOf(rowData));
                            break;
                        case DATE:
                            if(column instanceof Date) {
                                sb.append(DateUtil.dateToString((Date)column));
                            } else {
                                Date d = DateUtil.columnToDate(column);
                                String s = DateUtil.dateToString(d);
                                sb.append(s);
                            }
                            break;
                        case TIMESTAMP:
                            if(column instanceof Date) {
                                sb.append(DateUtil.timestampToString((Date)column));
                            } else {
                                Date d = DateUtil.columnToTimestamp(column);
                                String s = DateUtil.timestampToString(d);
                                sb.append(s);
                            }
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported column type: " + columnType);
                    }
                }
            }

            bytes = sb.toString().getBytes(this.charsetName);
            this.stream.write(bytes);
            this.stream.write(NEWLINE);
            nLine++;

            if(nLine % 1000 == 0) {
                this.stream.flush();
            }
        } catch(Exception e) {
            if(i < row.getArity()) {
                throw new WriteRecordException(recordConvertDetailErrorMessage(i, row), e, i, row);
            }
            throw new WriteRecordException(e.getMessage(), e);
        }

    }

    @Override
    protected String recordConvertDetailErrorMessage(int pos, Row row) {
        return "\nHdfsTextOutputFormat [" + jobName + "] writeRecord error: when converting field[" + columnNames.get(pos) + "] in Row(" + row + ")";
    }

    @Override
    public void closeInternal() throws IOException {
        OutputStream s = this.stream;
        if(s != null) {
            s.flush();
            this.stream = null;
            s.close();
        }
    }

}
