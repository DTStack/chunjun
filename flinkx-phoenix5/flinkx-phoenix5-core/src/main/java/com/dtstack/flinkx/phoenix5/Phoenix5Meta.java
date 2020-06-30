package com.dtstack.flinkx.phoenix5;

import com.dtstack.flinkx.phoenix.PhoenixMeta;

public class Phoenix5Meta extends PhoenixMeta {
    @Override
    public String getSplitFilter(String columnName) {
        return String.format("%s %% ${N} = ${M}", getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public String getSplitFilterWithTmpTable(String tmpTable, String columnName){
        return String.format("%s.%s %% ${N} = ${M}", tmpTable, getStartQuote() + columnName + getEndQuote());
    }
}
