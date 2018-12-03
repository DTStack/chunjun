package com.dtstack.flinkx.carbondata.writer;


import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import java.util.ArrayList;
import java.util.List;


public class CarbonDictionaryUtil {

    private CarbonDictionaryUtil() {
        // hehe
    }

    /**
     * generate global dictionary with SQLContext and CarbonLoadModel
     *
     * @param carbonLoadModel carbon load model
     */
    public static void generateGlobalDictionary(CarbonLoadModel carbonLoadModel, Configuration hadoopConf) {
        CarbonTable carbonTable = carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable();
        CarbonTableIdentifier carbonTableIdentifier = carbonTable.getAbsoluteTableIdentifier().getCarbonTableIdentifier();
        String dictfolderPath = CarbonTablePath.getMetadataPath(carbonLoadModel.getTablePath());
        List<CarbonDimension> dimensions = carbonTable.getDimensionByTableName(carbonTable.getTableName());
        carbonLoadModel.initPredefDictMap();
        String allDictionaryPath = carbonLoadModel.getAllDictPath();

        if (StringUtils.isEmpty(allDictionaryPath)) {
            String[] headers = carbonLoadModel.getCsvHeaderColumns();
            for(int i = 0; i < headers.length; ++i) {
                headers[i] = headers[i].trim();
            }
            String colDictFilePath = carbonLoadModel.getColDictFilePath();
            Pair pair = pruneDimensions(dimensions, headers,headers);
            List<CarbonDimension> requireDimension = pair.dimensions;
            List<String> requireColumnNames = pair.columnNames;
            if(!requireColumnNames.isEmpty()) {

            }
        }
    }

    /**
     * find columns which need to generate global dictionary.
     *
     * @param dimensions dimension list of schema
     * @param headers    column headers
     * @param columns    column list of csv file
     */
    private static Pair pruneDimensions(List<CarbonDimension> dimensions, String[] headers, String[] columns) {
        List<CarbonDimension> dimensionBuffer = new ArrayList<>();
        List<String> columnNameBuffer = new ArrayList<>();
        List<CarbonDimension> dimensionsWithDict = new ArrayList<>();
        for(CarbonDimension dimension: dimensions) {
            if(hasEncoding(dimension, Encoding.DICTIONARY, Encoding.DIRECT_DICTIONARY)) {
                dimensionsWithDict.add(dimension);
            }
        }
        for(CarbonDimension dim : dimensionsWithDict) {
            for(int i = 0; i < headers.length; ++i) {
                String header = headers[i];
                if(header.equalsIgnoreCase(dim.getColName())) {
                    dimensionBuffer.add(dim);
                    columnNameBuffer.add(columns[i]);
                }
            }
        }
        return new Pair(dimensionBuffer, columnNameBuffer);
    }

    private static boolean hasEncoding(CarbonDimension dimension, Encoding encoding, Encoding excludeEncoding) {
        if(dimension.isComplex()) {
            throw new IllegalArgumentException("complex dimension is not supported");
        } else {
            return dimension.hasEncoding(encoding) &&
                    (excludeEncoding == null || !dimension.hasEncoding(excludeEncoding));
        }
    }


    static class Pair {

        List<CarbonDimension> dimensions;

        List<String> columnNames;

        public Pair(List<CarbonDimension> dimensions, List<String> columnNames) {
            this.dimensions = dimensions;
            this.columnNames = columnNames;
        }
    }

    static class DictionaryLoadModel {
        AbsoluteTableIdentifier table;

        List<CarbonDimension> dimensions;

        String hdfsLocation;

        String dictfolderPath;

        List<String> dictFilePaths;

        List<Boolean> dictFileExists;

        List<Boolean> isComplexes;

        List<CarbonDimension> primDimensions;
    }
}
