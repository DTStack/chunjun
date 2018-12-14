package com.dtstack.flinkx.carbondata.writer;


import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;

import java.util.List;

public class DictionaryLoadModel {

    AbsoluteTableIdentifier table;

    List<CarbonDimension> dimensions;

    String hdfsLocation;

    String dictfolderPath;

    String[] dictFilePaths;

    Boolean[] dictFileExists;

    List<Boolean> isComplexes;

    List<CarbonDimension> primDimensions;

    String[] delimiters;

    ColumnIdentifier[] columnIdentifier;

    boolean isFirstLoad;

    String hdfsTempLocation;

    String lockType;

    String zooKeeperUrl;

    String serializationNullFormat;

    String defaultTimestampFormat;

    String defaultDateFormat;

    public DictionaryLoadModel(
            AbsoluteTableIdentifier table,
            List<CarbonDimension> dimensions,
            String hdfsLocation,
            String dictfolderPath,
            String[] dictFilePaths,
            Boolean[] dictFileExists,
            List<Boolean> isComplexes,
            List<CarbonDimension> primDimensions,
            String[] delimiters,
            ColumnIdentifier[] columnIdentifier,
            boolean isFirstLoad,
            String hdfsTempLocation,
            String lockType,
            String zooKeeperUrl,
            String serializationNullFormat,
            String defaultTimestampFormat,
            String defaultDateFormat) {
        this.table = table;
        this.dimensions = dimensions;
        this.hdfsLocation = hdfsLocation;
        this.dictfolderPath = dictfolderPath;
        this.dictFilePaths = dictFilePaths;
        this.dictFileExists = dictFileExists;
        this.isComplexes = isComplexes;
        this.primDimensions = primDimensions;
        this.delimiters = delimiters;
        this.columnIdentifier = columnIdentifier;
        this.isFirstLoad = isFirstLoad;
        this.hdfsTempLocation = hdfsTempLocation;
        this.lockType = lockType;
        this.zooKeeperUrl = zooKeeperUrl;
        this.serializationNullFormat = serializationNullFormat;
        this.defaultTimestampFormat = defaultTimestampFormat;
        this.defaultDateFormat = defaultDateFormat;
    }

}
