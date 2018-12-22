package com.dtstack.flinkx.carbondata.writer.dict;


import com.dtstack.flinkx.carbondata.writer.dict.GenericParser;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.processing.loading.exception.NoRetryException;
import java.util.HashSet;
import java.util.Set;


public class PrimitiveParser extends GenericParser {

    private boolean hasDictEncoding;

    private Set<String> set;

    public PrimitiveParser(CarbonDimension dimension, Set<String> setOpt) {
        if(setOpt == null) {
            hasDictEncoding = false;
            set = new HashSet<>();
        } else {
            hasDictEncoding = true;
            set = setOpt;
        }
    }

    @Override
    public void addChild(GenericParser child) {

    }

    @Override
    public void parseString(String input) {
        if (hasDictEncoding && input != null) {
            if (set.size() < CarbonLoadOptionConstants.MAX_EXTERNAL_DICTIONARY_SIZE) {
                set.add(input);
            } else {
                throw new NoRetryException("Cannot provide more than " + CarbonLoadOptionConstants.MAX_EXTERNAL_DICTIONARY_SIZE + " dictionary values");
            }
        }
    }

}
