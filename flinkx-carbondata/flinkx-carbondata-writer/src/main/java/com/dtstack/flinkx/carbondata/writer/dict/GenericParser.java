package com.dtstack.flinkx.carbondata.writer.dict;


import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;

public abstract class GenericParser {

    protected CarbonDimension dimension;

    public CarbonDimension getDimension() {
        return dimension;
    }

    public void setDimension(CarbonDimension dimension) {
        this.dimension = dimension;
    }

    public abstract void addChild(GenericParser child);

    public abstract void parseString(String input);

}
