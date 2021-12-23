package com.dtstack.flinkx.alluxio.writer;

import com.dtstack.flinkx.alluxio.util.StringUtil;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.enums.WriteType;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.*;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_COLUMN_NAME;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_COLUMN_TYPE;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_COMPRESS;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_ENABLE_DICTIONARY;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_ENCODING;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_FIELD_DELIMITER;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_FILE_NAME;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_FILE_TYPE;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_FLUSH_INTERVAL;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_FULL_COLUMN_NAME_LIST;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_FULL_COLUMN_TYPE_LIST;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_MAX_FILE_SIZE;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_PATH;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_ROW_GROUP_SIZE;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_WRITE_MODE;
import static com.dtstack.flinkx.alluxio.AlluxioConfigKeys.KEY_WRITE_TYPE;

/**
 * @author wuzhongjian_yewu@cmss.chinamobile.com
 * @date 2021-12-06
 */
public class AlluxioWriter extends BaseDataWriter {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected String fileType;

    protected String path;

    protected String fieldDelimiter;

    protected String compress;

    protected String fileName;

    protected List<String> columnName;

    protected List<String> columnType;

    protected String charSet;

    protected List<String> fullColumnName;

    protected List<String> fullColumnType;

    protected int rowGroupSize;

    protected long maxFileSize;

    protected long flushInterval;

    protected boolean enableDictionary;

    protected String writerType;

    public AlluxioWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        List columns = writerConfig.getParameter().getColumn();
        fileType = writerConfig.getParameter().getStringVal(KEY_FILE_TYPE);
        path = writerConfig.getParameter().getStringVal(KEY_PATH);
        fieldDelimiter = writerConfig.getParameter().getStringVal(KEY_FIELD_DELIMITER);
        charSet = writerConfig.getParameter().getStringVal(KEY_ENCODING);
        rowGroupSize = writerConfig.getParameter().getIntVal(KEY_ROW_GROUP_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE);
        maxFileSize = writerConfig.getParameter().getLongVal(KEY_MAX_FILE_SIZE, ConstantValue.STORE_SIZE_G);
        flushInterval = writerConfig.getParameter().getLongVal(KEY_FLUSH_INTERVAL, 0);
        enableDictionary = writerConfig.getParameter().getBooleanVal(KEY_ENABLE_DICTIONARY, true);
        writerType = writerConfig.getParameter().getStringVal(KEY_WRITE_TYPE, WriteType.THROUGH.name());

        if (fieldDelimiter == null || fieldDelimiter.length() == 0) {
            fieldDelimiter = "\001";
        } else {
            fieldDelimiter = com.dtstack.flinkx.util.StringUtil.convertRegularExpr(fieldDelimiter);
        }

        compress = writerConfig.getParameter().getStringVal(KEY_COMPRESS);
        fileName = writerConfig.getParameter().getStringVal(KEY_FILE_NAME, "");
        if (columns != null && columns.size() > 0) {
            columnName = new ArrayList<>();
            columnType = new ArrayList<>();
            for (Object column : columns) {
                Map sm = (Map) column;
                columnName.add((String) sm.get(KEY_COLUMN_NAME));
                columnType.add((String) sm.get(KEY_COLUMN_TYPE));
            }
        }

        fullColumnName = (List<String>) writerConfig.getParameter().getVal(KEY_FULL_COLUMN_NAME_LIST);
        fullColumnType = (List<String>) writerConfig.getParameter().getVal(KEY_FULL_COLUMN_TYPE_LIST);

        mode = writerConfig.getParameter().getStringVal(KEY_WRITE_MODE);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        AlluxioOutputFormatBuilder builder = new AlluxioOutputFormatBuilder(fileType);
        builder.setPath(formatPath(path));
        builder.setFileName(fileName);
        builder.setWriteMode(mode);
        builder.setColumnNames(columnName);
        builder.setColumnTypes(columnType);
        builder.setCompress(compress);
        builder.setMonitorUrls(monitorUrls);
        builder.setErrors(errors);
        builder.setErrorRatio(errorRatio);
        builder.setFullColumnNames(fullColumnName);
        builder.setFullColumnTypes(fullColumnType);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);
        builder.setCharSetName(charSet);
        builder.setDelimiter(fieldDelimiter);
        builder.setRowGroupSize(rowGroupSize);
        builder.setRestoreConfig(restoreConfig);
        builder.setMaxFileSize(maxFileSize);
        builder.setFlushBlockInterval(flushInterval);
        builder.setEnableDictionary(enableDictionary);
        builder.setWriteType(writerType);
        return createOutput(dataSet, builder.finish());
    }

    private String formatPath(String path) {
        String pathAfterFormat = path;
        if (!StringUtil.startsWith(path, "alluxio://")) {
            if (StringUtil.startsWith(path, "//")) {
                pathAfterFormat = "alluxio:" + path;
            } else if (StringUtil.startsWith(path, "/")) {
                pathAfterFormat = "alluxio:/" + path;
            } else {
                pathAfterFormat = "alluxio://" + path;
            }
        }

        if (!StringUtil.endsWith(pathAfterFormat, "/")) {
            pathAfterFormat = pathAfterFormat + "/";
        }

        LOG.debug("Path = " + pathAfterFormat);
        return pathAfterFormat;
    }
}