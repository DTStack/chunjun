package com.dtstack.flinkx.metadata.hive2.common;

/**
 * @author : tiezhu
 * @date : 2020/3/9
 * @description :
 */
public class Hive2MetaDataCons {
    public static final String TYPE_TEXT = "TextInputFormat";
    public static final String TYPE_PARQUET = "MapredParquetInputFormat";

    public static final String KEY_TABLE_PROPERTIES = "tableProperties";
    public static final String KEY_PARTITION_COLUMN = "partitionColumn";
    public static final String KEY_INPUT_FORMAT = "inputFormat";
}
