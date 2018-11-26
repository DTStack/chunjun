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

import com.dtstack.flinkx.carbondata.CarbondataUtil;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.DataLoadExecutor;
import org.apache.carbondata.processing.loading.FailureCauses;
import org.apache.carbondata.processing.loading.TableProcessingOperations;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.model.CarbonLoadModelBuilder;
import org.apache.carbondata.processing.loading.model.LoadOption;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CarbonOutputFormat extends RichOutputFormat {

    protected Map<String,String> hadoopConfig;

    protected String table;

    protected String database;

    protected String path;

    protected List<String> column;

    private CarbonTable carbonTable;

    private Map<String,String> options = new HashMap<>();

    private String metadataDirectoryPath;

    private CarbonLoadModel carbonLoadModel = new CarbonLoadModel();

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        CarbondataUtil.initFileFactory(hadoopConfig);
        carbonTable = CarbondataUtil.buildCarbonTable(database, table, path);
        buildOptions();
        System.out.println(carbonTable);
        CarbonProperties carbonProperty = CarbonProperties.getInstance();
        carbonProperty.addProperty("zookeeper.enable.lock", "false");
        buildCarbonLoadModel();
        TableProcessingOperations.deletePartialLoadDataIfExist(carbonTable, false);
        SegmentStatusManager.deleteLoadsAndUpdateMetadata(carbonTable, false, null);
        boolean isOverwriteTable = false;
        CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(carbonLoadModel, isOverwriteTable);
        if (carbonLoadModel.isCarbonTransactionalTable()) {
            metadataDirectoryPath = CarbonTablePath.getMetadataPath(carbonTable.getTablePath());
        }
        List<CarbonDimension> allDimensions = carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getAllDimensions();

        boolean createDictionary = false;
        if (!createDictionary) {
            carbonLoadModel.setUseOnePass(false);
        }

        carbonLoadModel.setSegmentId(String.valueOf(System.currentTimeMillis()));

    }

    private void loadData() {
        LoadMetadataDetails loadMetadataDetails = new LoadMetadataDetails();
        //ExecutionErrors executionErrors = new ExecutionErrors(FailureCauses.NONE, "");
        DataLoadExecutor executor = new DataLoadExecutor();
        String[] storeLocation = new String[] {"/tmp/woca.dat"};
        //executor.execute(, storeLocation, recordReaders.toArray)
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        System.out.println(row);
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {

    }

    private void buildOptions() {
        options.put("fileheader", StringUtils.join(column, ","));
    }

    private CarbonLoadModel buildCarbonLoadModel() {
        Map<String,String> tableProperties = carbonTable.getTableInfo().getFactTable().getTableProperties();
        try {
            Map<String,String> optionsFinal = LoadOption.fillOptionWithDefaultValue(options);
            optionsFinal.put("sort_scope", tableProperties.getOrDefault("sort_scope", CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT));
            carbonLoadModel.setParentTablePath(null);
            carbonLoadModel.setFactFilePath("");
            carbonLoadModel.setCarbonTransactionalTable(carbonTable.getTableInfo().isTransactionalTable());
            carbonLoadModel.setAggLoadRequest(false);
            carbonLoadModel.setSegmentId("");

            new CarbonLoadModelBuilder(carbonTable).build(
                    options,
                    optionsFinal,
                    carbonLoadModel,
                    FileFactory.getConfiguration(),
                    new HashMap<>(0),
                    true
            );


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return carbonLoadModel;
    }
}
