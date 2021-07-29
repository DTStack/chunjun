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


package com.dtstack.flinkx.carbondata.writer.dict;


import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.exception.NoRetryException;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * @author demotto
 */
public class CarbonDictionaryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(CarbonDictionaryUtil.class);

    private CarbonDictionaryUtil() {
        // hehe
    }

    /**
     * generate global dictionary with SQLContext and CarbonLoadModel
     * 入口
     *
     * @param carbonLoadModel carbon load model
     */
    public static void generateGlobalDictionary(CarbonLoadModel carbonLoadModel, List<String[]> data) throws IOException {

        CarbonTable carbonTable = carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable();
        CarbonTableIdentifier carbonTableIdentifier = carbonTable.getAbsoluteTableIdentifier().getCarbonTableIdentifier();
        String dictfolderPath = CarbonTablePath.getMetadataPath(carbonLoadModel.getTablePath());
        List<CarbonDimension> dimensionList = carbonTable.getDimensionByTableName(carbonTable.getTableName());
        CarbonDimension[] dimensions = dimensionList.toArray(new CarbonDimension[dimensionList.size()]);
        carbonLoadModel.initPredefDictMap();
        String[] headers = carbonLoadModel.getCsvHeaderColumns();
        Tuple2<CarbonDimension[],String[]> tuple2 = pruneDimensions(dimensions, headers,headers);
        CarbonDimension[] requireDimension = tuple2.getField(0);
        String[] requireColumnNames = tuple2.getField(1);

        if(requireColumnNames == null || requireColumnNames.length == 0) {
            LOG.info("no dictionary table");
            return;
        }
        DictionaryLoadModel model = createDictionaryLoadModel(carbonLoadModel, carbonTableIdentifier, requireDimension, dictfolderPath, false);

        int[] dictColIndices = new int[requireColumnNames.length];
        for(int i = 0; i < dictColIndices.length; ++i) {
            String dictColName = requireColumnNames[i];
            int j = 0;
            for(; j < headers.length; ++j) {
                if(dictColName.equalsIgnoreCase(headers[j])) {
                    break;
                }
            }
            if(j != headers.length) {
                dictColIndices[i] = j;
            } else {
                throw new RuntimeException("dict column " + dictColName + " is not included in headers");
            }
        }

        for(int i = 0; i < requireColumnNames.length; ++i) {
            int dictColIdx = dictColIndices[i];
            Set<String> distinctValues = data.stream().map(row -> row[dictColIdx]).collect(Collectors.toSet());
            writeDictionary(distinctValues, model, i);
        }

        CacheProvider cacheProvider = CacheProvider.getInstance();
        cacheProvider.dropAllCache();

    }

    private static SegmentStatus writeDictionary(Set<String> valuesBuffer, DictionaryLoadModel model, int idx) throws IOException {
        SegmentStatus status = SegmentStatus.SUCCESS;
        boolean dictionaryForDistinctValueLookUpCleared = false;
        DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier = new DictionaryColumnUniqueIdentifier(
                model.getTable(),
                model.getColumnIdentifier()[idx],
                model.getColumnIdentifier()[idx].getDataType()
        );

        ICarbonLock dictLock = CarbonLockFactory.getCarbonLockObj(model.table, model.columnIdentifier[idx].getColumnId() + LockUsage.LOCK);
        boolean isDictionaryLocked = false;
        Dictionary dictionaryForDistinctValueLookUp = null;

        try {
            isDictionaryLocked = dictLock.lockWithRetries();
            if(isDictionaryLocked) {
                LOG.info("Successfully able to get the dictionary lock for " + model.primDimensions.get(idx).getColName());
            } else {
                LOG.error("Dictionary file ");
                throw new RuntimeException();
            }

            FileFactory.FileType fileType = FileFactory.getFileType(model.getDictFilePaths()[idx]);
            boolean isDictFileExists = FileFactory.isFileExist(model.getDictFilePaths()[idx], fileType);
            if(isDictFileExists) {
                dictionaryForDistinctValueLookUp = CarbonLoaderUtil.getDictionary(model.getTable(), model.getColumnIdentifier()[idx], model.primDimensions.get(idx).getDataType());
            }

            DictionaryWriterTask dictWriteTask = new DictionaryWriterTask(valuesBuffer, dictionaryForDistinctValueLookUp, dictionaryColumnUniqueIdentifier, model.primDimensions.get(idx).getColumnSchema(), isDictFileExists);
            List<String> distinctValues = dictWriteTask.execute();

            if(distinctValues.size() > 0) {
                SortIndexWriterTask sortIndexWriteTask = new SortIndexWriterTask(dictionaryColumnUniqueIdentifier, model.primDimensions.get(idx).getDataType(), dictionaryForDistinctValueLookUp, distinctValues);
                sortIndexWriteTask.execute();
            }

            dictWriteTask.updateMetaData();
            valuesBuffer.clear();
            CarbonUtil.clearDictionaryCache(dictionaryForDistinctValueLookUp);
            dictionaryForDistinctValueLookUpCleared = true;
        } catch (NoRetryException dictionaryException) {
            LOG.error(dictionaryException.getMessage());
            status = SegmentStatus.LOAD_FAILURE;
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            throw ex;
        } finally {
            if (!dictionaryForDistinctValueLookUpCleared) {
                CarbonUtil.clearDictionaryCache(dictionaryForDistinctValueLookUp);
            }
            if (dictLock != null && isDictionaryLocked) {
                if (dictLock.unlock()) {
                    LOG.info("Dictionary " + model.primDimensions.get(idx).getColName() + " unlocked unsuccessfully.");
                }
            } else {
                LOG.error("Unable to unlock Dictionary " + model.primDimensions.get(idx).getColName());
            }
        }
        return status;
    }

    /**
     * find columns which need to generate global dictionary.
     *
     * @param dimensions dimension list of schema
     * @param headers    column headers
     * @param columns    column list of csv file
     */
    public static Tuple2<CarbonDimension[],String[]> pruneDimensions(CarbonDimension[] dimensions, String[] headers, String[] columns) {
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
        CarbonDimension[] dimensionArr = dimensionBuffer.toArray(new CarbonDimension[dimensionBuffer.size()]);
        dimensionBuffer.toArray(new CarbonDimension[dimensionBuffer.size()]);
        String[] columnNameArr = columnNameBuffer.toArray(new String[columnNameBuffer.size()]);

        return new Tuple2<>(dimensionArr, columnNameArr);
    }

    private static boolean hasEncoding(CarbonDimension dimension, Encoding encoding, Encoding excludeEncoding) {
        if(dimension.isComplex()) {
            throw new IllegalArgumentException("complex dimension is not supported");
        } else {
            return dimension.hasEncoding(encoding) &&
                    (excludeEncoding == null || !dimension.hasEncoding(excludeEncoding));
        }
    }

    public static DictionaryLoadModel createDictionaryLoadModel(
            CarbonLoadModel carbonLoadModel, CarbonTableIdentifier table,
            CarbonDimension[] dimensions, String dictFolderPath,
            boolean forPreDefDict) {
        List<CarbonDimension> primDimensionsBuffer = new ArrayList<>();
        List<Boolean> isComplexes = new ArrayList<>();

        for(int i = 0; i < dimensions.length; ++i) {
            List<CarbonDimension> dims = getPrimDimensionWithDict(carbonLoadModel, dimensions[i], forPreDefDict);
            for(int j = 0; j < dims.size(); ++j) {
                primDimensionsBuffer.add(dims.get(j));
                isComplexes.add(dimensions[i].isComplex());
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

}
