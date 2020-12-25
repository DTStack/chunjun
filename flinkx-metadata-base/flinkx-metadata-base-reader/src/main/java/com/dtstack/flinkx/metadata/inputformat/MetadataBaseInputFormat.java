/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.metadata.inputformat;

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.metadata.constants.BaseConstants;
import com.dtstack.flinkx.metadata.entity.MetadataEntity;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.metadata.constants.BaseConstants.DEFAULT_OPERA_TYPE;

/**
 * @author kunni@dtstack.com
 */
abstract public class MetadataBaseInputFormat extends BaseRichInputFormat {

    protected List<Map<String, Object>> originalJob;

    protected int currentPosition;

    protected Iterator<Object> iterator;

    protected Object currentObject;

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        currentPosition = 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected InputSplit[] createInputSplitsInternal(int splitNumber) throws Exception {
        InputSplit[] inputSplits = new MetadataBaseInputSplit[originalJob.size()];
        for (int index = 0; index < originalJob.size(); index++) {
            Map<String, Object> dbTables = originalJob.get(index);
            String dbName = MapUtils.getString(dbTables, BaseConstants.KEY_DB_NAME);
            if(StringUtils.isNotEmpty(dbName)){
                List<Object> tables = (List<Object>) dbTables.get(BaseConstants.KEY_TABLE_LIST);
                inputSplits[index] = new MetadataBaseInputSplit(splitNumber, dbName, tables);
            }
        }
        return inputSplits;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        currentObject = iterator.next();
        MetadataEntity metadataEntity = createMetadataEntity();
        metadataEntity.setOperaType(DEFAULT_OPERA_TYPE);
        currentPosition++;

        return Row.of(metadataEntity);
    }

    /**
     * 创建元数据实体类
     * @return metadataEntity
     */
    public abstract MetadataEntity createMetadataEntity();

    @Override
    protected void closeInternal() {
    }

    @Override
    public boolean reachedEnd() {
        return iterator.hasNext();
    }

    public void setOriginalJob(List<Map<String, Object>> originalJob){
        this.originalJob = originalJob;
    }

}
