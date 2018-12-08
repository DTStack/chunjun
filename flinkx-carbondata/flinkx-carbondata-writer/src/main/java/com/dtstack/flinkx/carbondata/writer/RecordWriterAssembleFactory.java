package com.dtstack.flinkx.carbondata.writer;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;


public class RecordWriterAssembleFactory {

    public AbstractRecordWriterAssemble getAssembleInstance(CarbonTable carbonTable) {
        if(carbonTable.isHivePartitionTable()) {
            return new HivePartitionRecordWriterAssemble(carbonTable);
        } else if(carbonTable.getPartitionInfo() == null) {
            return new SimpleRecordWriterAssemble(carbonTable);
        } else {
            return new CarbonPartitionRecordWriterAssemble(carbonTable);
        }
    }

}
