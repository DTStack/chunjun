package com.dtstack.flinkx.alluxio.writer;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.outputformat.FileOutputFormatBuilder;

import java.util.List;

/**
 * @author wuzhongjian_yewu@cmss.chinamobile.com
 * @date 2021-12-06
 */
public class AlluxioOutputFormatBuilder extends FileOutputFormatBuilder {

    private final BaseAlluxioOutputFormat format;

    public AlluxioOutputFormatBuilder(String type) {
        switch (type.toUpperCase()) {
            case "TEXT":
                format = new AlluxioTextOutputFormat();
                break;
            case "ORC":
                format = new AlluxioOrcOutputFormat();
                break;
            case "PARQUET":
                format = new AlluxioParquetOutputFormat();
                break;
            default:
                throw new IllegalArgumentException("Unsupported Alluxio file type: " + type);
        }

        super.setFormat(format);
    }

    public void setColumnNames(List<String> columnNames) {
        format.columnNames = columnNames;
    }

    public void setColumnTypes(List<String> columnTypes) {
        format.columnTypes = columnTypes;
    }

    public void setFullColumnNames(List<String> fullColumnNames) {
        format.fullColumnNames = fullColumnNames;
    }

    public void setDelimiter(String delimiter) {
        format.delimiter = delimiter;
    }

    public void setRowGroupSize(int rowGroupSize) {
        format.rowGroupSize = rowGroupSize;
    }

    public void setFullColumnTypes(List<String> fullColumnTypes) {
        format.fullColumnTypes = fullColumnTypes;
    }

    public void setEnableDictionary(boolean enableDictionary) {
        format.enableDictionary = enableDictionary;
    }

    public void setWriteType(String writeType) {
        format.writeType = writeType;
    }

    @Override
    protected void checkFormat() {
        super.checkFormat();

        if (super.format.getPath() == null || super.format.getPath().length() == 0) {
            throw new IllegalArgumentException("No valid path supplied.");
        }

        if (!super.format.getPath().startsWith(ConstantValue.PROTOCOL_ALLUXIO)) {
            throw new IllegalArgumentException("Path should start with alluxio://");
        }
    }

}
