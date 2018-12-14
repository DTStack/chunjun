package com.dtstack.flinkx.carbondata.writer;

import org.apache.flink.types.Row;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;


public class CarbonBlockDistinctValueCombiner {

    private List<String[]> rows;

    private DictionaryLoadModel model;

    public CarbonBlockDistinctValueCombiner(List<Row> rows, DictionaryLoadModel model) {
//        this.rows = rows;
        this.model = model;
    }

    public void combine() {
        List<DistinctValue> distinctValuesList = new ArrayList<>();
        long rowCount = 0L;
        GenericParser[] dimensionParsers = CarbonDictionaryUtil.createDimensionParsers(model, distinctValuesList);
        int dimNum = model.dimensions.size();
        SimpleDateFormat timeStampFormat = new SimpleDateFormat(model.defaultTimestampFormat);
        SimpleDateFormat dateFormat = new SimpleDateFormat(model.defaultDateFormat);

        for(String[] row : rows) {
            if(row != null) {
                rowCount++;
                for(int i = 0; i < dimNum; ++i) {
                    // FIX ME: need convert
//                    dimensionParsers[i].parseString();
                }
            }
        }
    }


}
