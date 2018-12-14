/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.dtstack.flinkx.carbondata.writer;


import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


/**
 * A object which provide a method to generate global dictionary from CSV files.
 */
public class CarbonDictionaryUtil {

    private CarbonDictionaryUtil() {
        // hehe
    }

    /**
     * generate global dictionary with SQLContext and CarbonLoadModel
     * 入口
     *
     * @param carbonLoadModel carbon load model
     */
    public static void generateGlobalDictionary(CarbonLoadModel carbonLoadModel, Configuration hadoopConf) {
        CarbonTable carbonTable = carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable();
        CarbonTableIdentifier carbonTableIdentifier = carbonTable.getAbsoluteTableIdentifier().getCarbonTableIdentifier();
        String dictfolderPath = CarbonTablePath.getMetadataPath(carbonLoadModel.getTablePath());
        List<CarbonDimension> dimensionList = carbonTable.getDimensionByTableName(carbonTable.getTableName());
        CarbonDimension[] dimensions = dimensionList.toArray(new CarbonDimension[dimensionList.size()]);
        carbonLoadModel.initPredefDictMap();
        String allDictionaryPath = carbonLoadModel.getAllDictPath();

        if (StringUtils.isEmpty(allDictionaryPath)) {
            String[] headers = carbonLoadModel.getCsvHeaderColumns();
            for(int i = 0; i < headers.length; ++i) {
                headers[i] = headers[i].trim();
            }
            String colDictFilePath = carbonLoadModel.getColDictFilePath();
            if(colDictFilePath != null) {
                generatePredefinedColDictionary(colDictFilePath, carbonTableIdentifier, dimensions, carbonLoadModel, dictfolderPath);
            }

            String[] headerOfInputData = headers;

            Pair pair = pruneDimensions(dimensions, headers,headers);
            List<CarbonDimension> requireDimension = pair.dimensions;
            List<String> requireColumnNames = pair.columnNames;
            if(!requireColumnNames.isEmpty()) {
                //dict rdd
                DictionaryLoadModel model = createDictionaryLoadModel(carbonLoadModel, carbonTableIdentifier, requireDimension, dictfolderPath, false);
            }
        }
    }

    private static void generatePredefinedColDictionary(String colDictFilePath, CarbonTableIdentifier table, CarbonDimension[] dimensions, CarbonLoadModel carbonLoadModel, String dictFolderPath) {
        new UnsupportedOperationException();
    }

    private static void loadInputDataAsDict(CarbonLoadModel carbonLoadModel) {

    }

    /**
     * find columns which need to generate global dictionary.
     *
     * @param dimensions dimension list of schema
     * @param headers    column headers
     * @param columns    column list of csv file
     */
    private static Pair pruneDimensions(CarbonDimension[] dimensions, String[] headers, String[] columns) {
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

    private static DictionaryLoadModel createDictionaryLoadModel(
            CarbonLoadModel carbonLoadModel, CarbonTableIdentifier table,
            List<CarbonDimension> dimensions, String dictFolderPath,
            boolean forPreDefDict) {
        List<CarbonDimension> primDimensionsBuffer = new ArrayList<>();
        List<Boolean> isComplexes = new ArrayList<>();

        for(int i = 0; i < dimensions.size(); ++i) {
            List<CarbonDimension> dims = getPrimDimensionWithDict(carbonLoadModel, dimensions.get(i), forPreDefDict);
            for(int j = 0; j < dims.size(); ++j) {
                primDimensionsBuffer.add(dims.get(j));
                isComplexes.add(dimensions.get(i).isComplex());
            }
        }

        List<CarbonDimension> primDimensions = primDimensionsBuffer;
        DictionaryDetail dictDetail = DictionaryDetailHelper.getDictionaryDetail(dictFolderPath, primDimensions, carbonLoadModel.getTablePath());
        String[] dictFilePaths = dictDetail.dictFilePaths;
        Boolean[] dictFileExists = dictDetail.dictFileExists;
        ColumnIdentifier[] columnIdentifier = dictDetail.columnIdentifiers;
        String hdfsTempLocation = CarbonProperties.getInstance().
                getProperty(CarbonCommonConstants.HDFS_TEMP_LOCATION, System.getProperty("java.io.tmpdir"));
        String lockType = CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.LOCK_TYPE, CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS);
        String zookeeperUrl = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.ZOOKEEPER_URL);
        String serializationNullFormat = carbonLoadModel.getSerializationNullFormat().split(CarbonCommonConstants.COMMA, 2)[1];
        // get load count
        if (null == carbonLoadModel.getLoadMetadataDetails()) {
            carbonLoadModel.readAndSetLoadMetadataDetails();
        }
        AbsoluteTableIdentifier absoluteTableIdentifier = AbsoluteTableIdentifier.from(carbonLoadModel.getTablePath(), table);
        DictionaryLoadModel dictionaryLoadModel = new DictionaryLoadModel(
                absoluteTableIdentifier,
                dimensions,
                carbonLoadModel.getTablePath(),
                dictFolderPath,
                dictFilePaths,
                dictFileExists,
                isComplexes,
                primDimensions,
                carbonLoadModel.getDelimiters(),
                columnIdentifier,
                carbonLoadModel.getLoadMetadataDetails().size() == 0,
                hdfsTempLocation,
                lockType,
                zookeeperUrl,
                serializationNullFormat,
                carbonLoadModel.getDefaultTimestampFormat(),
                carbonLoadModel.getDefaultDateFormat()
        );
        return dictionaryLoadModel;
    }

    private static List<CarbonDimension> getPrimDimensionWithDict(CarbonLoadModel carbonLoadModel, CarbonDimension dimension, boolean forPreDefDict) {
        List<CarbonDimension> dimensionsWithDict = new ArrayList<>();
        gatherDimensionByEncoding(carbonLoadModel,
                dimension,Encoding.DICTIONARY,
                Encoding.DIRECT_DICTIONARY,
                dimensionsWithDict, forPreDefDict);
        return dimensionsWithDict;
    }

    private static void gatherDimensionByEncoding(CarbonLoadModel carbonLoadModel, CarbonDimension dimension, Encoding encoding, Encoding excludeEncoding, final List<CarbonDimension> dimensionsWithEncoding, boolean forPreDefDict) {
        if(dimension.isComplex()) {
            List<CarbonDimension> children = dimension.getListOfChildDimensions();
            children.forEach(c -> {
                gatherDimensionByEncoding(carbonLoadModel, c, encoding, excludeEncoding, dimensionsWithEncoding, forPreDefDict);
            });
        } else {
            if(dimension.hasEncoding(encoding)) {
                if(excludeEncoding == null || !dimension.hasEncoding(excludeEncoding)) {
                    if(forPreDefDict && carbonLoadModel.getPredefDictFilePath(dimension) != null) {
                        dimensionsWithEncoding.add(dimension);
                    } else if(!forPreDefDict && carbonLoadModel.getPredefDictFilePath(dimension) == null) {
                        dimensionsWithEncoding.add(dimension);
                    }
                }
            }
        }
    }


    /**
     * generate Dimension Parsers
     *
     * @param model
     * @param distinctValuesList
     * @return dimensionParsers
     */
    public static GenericParser[] createDimensionParsers(DictionaryLoadModel model, List<DistinctValue> distinctValuesList) {
        // local combine set
        int dimNum = model.dimensions.size();
        int primDimNum = model.primDimensions.size();
        HashSet<String>[] columnValues = new HashSet[primDimNum];
        Map<String, HashSet<String>> mapColumnValuesWithId = new HashMap<>();
        for(int i = 0; i < primDimNum; ++i) {
            columnValues[i] = new HashSet();
            distinctValuesList.add(new DistinctValue(i, columnValues[i]));
            mapColumnValuesWithId.put(model.primDimensions.get(i).getColumnId(), columnValues[i]);
        }
        GenericParser[] dimensionParsers = new GenericParser[dimNum];
        for(int j = 0; j < dimNum; ++j) {
            dimensionParsers[j] = generateParserForDimension(model.dimensions.get(j), mapColumnValuesWithId);
        }
        return dimensionParsers;
    }

    private static GenericParser generateParserForDimension(CarbonDimension dimension, Map<String, HashSet<String>> mapColumnValuesWithId) {
        if(dimension == null) {
            return null;
        } else if(DataTypes.isArrayType(dimension.getDataType()) || DataTypes.isStructType(dimension.getDataType())) {
            throw new UnsupportedOperationException();
        } else {
            return new PrimitiveParser(dimension, mapColumnValuesWithId.get(dimension.getColumnId()));
        }
    }


}
