package com.dtstack.flinkx.metadatasync.reader.inputformat;

/**
 * @author : tiezhu
 * @date : 2020/3/7
 * @description : 记录元数据采集过程中涉及到的固定参数
 */
public class  MetaDataCons {
    public static final String KEY_INPUT_FORMAT = "InputFormat";
    public static final String KEY_COLUMN = "column";
    public static final String KEY_PARTITION_COLUMN = "partitionColumn";
    public static final String KEY_COMMENT = "comment";
    public static final String KEY_STORED_TYPE = "storedType";
    public static final String KEY_TABLE_PROPERTIES = "tableProperties";
    public static final String KEY_OPERA_TYPE = "operaType";
    public static final String KEY_TABLE = "table";

    public static final String KEY_COLUMN_NAME = "name";
    public static final String KEY_COLUMN_INDEX = "index";
    public static final String KEY_COLUMN_COMMENT = "comment";
    public static final String KEY_COLUMN_TYPE = "type";

    public static final String TYPE_TEXT = "TextInputFormat";
    public static final String TYPE_PARQUET = "MapredParquetInputFormat";

}
