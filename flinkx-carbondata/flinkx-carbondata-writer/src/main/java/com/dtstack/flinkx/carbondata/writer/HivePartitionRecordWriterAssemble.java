package com.dtstack.flinkx.carbondata.writer;


import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

public class HivePartitionRecordWriterAssemble extends AbstractRecordWriterAssemble {

    public HivePartitionRecordWriterAssemble(CarbonTable carbonTable) {
        super(carbonTable);
    }

    @Override
    protected int getRecordWriterNumber(Object[] record) {

        return 0;
    }


    @Override
    protected void postCloseRecordWriter() {

    }

}
