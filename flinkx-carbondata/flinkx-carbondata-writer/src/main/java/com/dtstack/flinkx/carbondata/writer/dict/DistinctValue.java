package com.dtstack.flinkx.carbondata.writer.dict;


import java.util.HashSet;

public class DistinctValue {

    int index;

    HashSet<String> columnValues;

    public DistinctValue(int index, HashSet<String> columnValues) {
        this.index = index;
        this.columnValues = columnValues;
    }

}
