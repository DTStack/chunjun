package com.dtstack.flinkx.carbondata.writer.recordwriter;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;


public class RecordWriterFactory {

    public static AbstractRecordWriter getAssembleInstance(CarbonTable carbonTable, String partition) {
        if(carbonTable.isHivePartitionTable()) {
            return new HivePartitionRecordWriter(carbonTable, partition);
        } else if(carbonTable.getPartitionInfo() == null) {
            return new SimpleRecordWriter(carbonTable);
        } else {
            return new CarbonPartitionRecordWriter(carbonTable);
        }
    }

}
