package com.dtstack.flinkx.carbondata.writer;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;


public class RecordWriterAssembleFactory {

    public static AbstractRecordWriterAssemble getAssembleInstance(CarbonTable carbonTable, String partition) {
        if(carbonTable.isHivePartitionTable()) {
            return new HivePartitionRecordWriterAssemble(carbonTable, partition);
        } else if(carbonTable.getPartitionInfo() == null) {
            return new SimpleRecordWriterAssemble(carbonTable);
        } else {
            return new CarbonPartitionRecordWriterAssemble(carbonTable);
        }
    }

}
