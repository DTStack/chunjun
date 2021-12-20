package com.dtstack.flinkx.alluxio.writer;

import com.dtstack.flinkx.alluxio.AlluxioUtil;
import com.dtstack.flinkx.alluxio.ECompressType;
import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author wuzhongjian_yewu@cmss.chinamobile.com
 * @date 2021-12-06
 */
public class AlluxioTextOutputFormat extends BaseAlluxioOutputFormat {

    private static final int NEWLINE = 10;
    private transient OutputStream stream;

    private static final int BUFFER_SIZE = 1000;

    @Override
    protected void flushDataInternal() throws IOException {
        LOG.info("Close current text stream, write data size:[{}]", bytesWriteCounter.getLocalValue());

        if (stream != null) {
            stream.flush();
            stream.close();
            stream = null;
        }
    }

    @Override
    public float getDeviation() {
        ECompressType compressType = ECompressType.getByTypeAndFileType(compress, "text");
        return compressType.getDeviation();
    }

    @Override
    protected String getExtension() {
        ECompressType compressType = ECompressType.getByTypeAndFileType(compress, "text");
        return compressType.getSuffix();
    }

    @Override
    protected void nextBlock() {
        super.nextBlock();

        if (stream != null) {
            return;
        }

        try {
            String currentBlockTmpPath = tmpPath + SP + currentBlockFileName;
            Path p = new Path(currentBlockTmpPath);

            ECompressType compressType = ECompressType.getByTypeAndFileType(compress, "text");
            if (ECompressType.TEXT_NONE.equals(compressType)) {
                stream = fs.create(p);
            } else {
                p = new Path(currentBlockTmpPath);
                if (compressType == ECompressType.TEXT_GZIP) {
                    stream = new GzipCompressorOutputStream(fs.create(p));
                } else if (compressType == ECompressType.TEXT_BZIP2) {
                    stream = new BZip2CompressorOutputStream(fs.create(p));
                } else if (compressType == ECompressType.TEXT_LZO) {
                    CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
                    stream = factory.getCodecByClassName("com.hadoop.compression.lzo.LzopCodec").createOutputStream(fs.create(p));
                }
            }

            LOG.info("subtask:[{}] create block file:{}", taskNumber, currentBlockTmpPath);

            blockIndex++;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void writeSingleRecordToFile(Row row) throws WriteRecordException {
        if (stream == null) {
            nextBlock();
        }

        StringBuilder sb = new StringBuilder();
        int i = 0;
        try {
            int cnt = fullColumnNames.size();
            for (; i < cnt; ++i) {
                int j = colIndices[i];
                if (j == -1) {
                    continue;
                }

                if (i != 0) {
                    sb.append(delimiter);
                }

                appendDataToString(sb, row.getField(j), ColumnType.fromString(columnTypes.get(j)));
            }
        } catch (Exception e) {
            if (i < row.getArity()) {
                throw new WriteRecordException(recordConvertDetailErrorMessage(i, row), e, i, row);
            }
            throw new WriteRecordException(e.getMessage(), e);
        }

        try {
            byte[] bytes = sb.toString().getBytes(this.charsetName);
            this.stream.write(bytes);
            this.stream.write(NEWLINE);
            rowsOfCurrentBlock++;

            if (restoreConfig.isRestore()) {
                lastRow = row;
            }

            if (rowsOfCurrentBlock % BUFFER_SIZE == 0) {
                this.stream.flush();
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new WriteRecordException(String.format("数据写入Alluxio异常，row:{%s}", row), e);
        }
    }

    private void appendDataToString(StringBuilder sb, Object column, ColumnType columnType) {
        if (column == null) {
            sb.append(AlluxioUtil.NULL_VALUE);
            return;
        }

        String rowData = column.toString();
        if (rowData.length() == 0) {
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
                    if (column instanceof Timestamp) {
                        column = ((Timestamp) column).getTime();
                        sb.append(column);
                        break;
                    }

                    BigInteger data = new BigInteger(rowData);
                    if (data.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) > 0) {
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
                    if (column instanceof Timestamp) {
                        SimpleDateFormat fm = DateUtil.getDateTimeFormatterForMillisencond();
                        sb.append(fm.format(column));
                    } else if (column instanceof Map || column instanceof List) {
                        sb.append(gson.toJson(column));
                    } else {
                        sb.append(rowData);
                    }
                    break;
                case BOOLEAN:
                    sb.append(StringUtil.parseBoolean(rowData));
                    break;
                case DATE:
                    column = DateUtil.columnToDate(column, null);
                    sb.append(DateUtil.dateToString((Date) column));
                    break;
                case TIMESTAMP:
                    column = DateUtil.columnToTimestamp(column, null);
                    sb.append(DateUtil.timestampToString((Date) column));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported column type: " + columnType);
            }
        }
    }

    @Override
    protected String recordConvertDetailErrorMessage(int pos, Row row) {
        return "\nAlluxioTextOutputFormat [" + jobName + "] writeRecord error: when converting field[" + columnNames.get(pos) + "] in Row(" + row + ")";
    }

    @Override
    public void closeSource() throws IOException {
        OutputStream s = this.stream;
        if (s != null) {
            s.flush();
            this.stream = null;
            s.close();
        }
    }

}
