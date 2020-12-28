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
import com.dtstack.flinkx.metadata.constants.BaseCons;
import com.dtstack.flinkx.metadata.entity.MetadataEntity;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.metadata.constants.BaseCons.DEFAULT_OPERA_TYPE;

/**
 * @author kunni@dtstack.com
 */
abstract public class MetadataBaseInputFormat extends BaseRichInputFormat {

    protected List<Map<String, Object>> originalJob;

    protected String currentDatabase;

    protected List<Object> tableList;

    protected int currentPosition;

    protected Iterator<Object> iterator;

    protected Object currentObject;

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        currentPosition = 0;
        LOG.info("inputSplit : {} ", inputSplit);
        tableList = ((MetadataBaseInputSplit) inputSplit).getTableList();
        currentDatabase = ((MetadataBaseInputSplit) inputSplit).getDbName();
        doOpenInternal();
        iterator = tableList.iterator();
    }

    /**
     * 建立连接，初始化设置
     * @throws IOException 异常
     */
    abstract protected void doOpenInternal() throws IOException;

    @SuppressWarnings("unchecked")
    @Override
    protected InputSplit[] createInputSplitsInternal(int splitNumber) {
        InputSplit[] inputSplits = new MetadataBaseInputSplit[originalJob.size()];
        for (int index = 0; index < originalJob.size(); index++) {
            Map<String, Object> dbTables = originalJob.get(index);
            String dbName = MapUtils.getString(dbTables, BaseCons.KEY_DB_NAME);
            if(StringUtils.isNotEmpty(dbName)){
                List<Object> tables = (List<Object>) dbTables.get(BaseCons.KEY_TABLE_LIST);
                inputSplits[index] = new MetadataBaseInputSplit(splitNumber, dbName, tables);
            }
        }
        return inputSplits;
    }

    @Override
    protected Row nextRecordInternal(Row row) {
        currentObject = iterator.next();
        MetadataEntity metadataEntity = new MetadataEntity();
        try{
            metadataEntity = createMetadataEntity();
            metadataEntity.setQuerySuccess(true);
        }catch (Exception e){
            metadataEntity.setQuerySuccess(false);
            metadataEntity.setErrorMsg(ExceptionUtil.getErrorMessage(e));
        }
        metadataEntity.setOperaType(DEFAULT_OPERA_TYPE);
        currentPosition++;
        return Row.of(GsonUtil.GSON.toJson(metadataEntity));
    }

    /**
     * 创建元数据实体类
     * @return metadataEntity 表的元数据
     * @throws IOException 异常
     */
    public abstract MetadataEntity createMetadataEntity() throws IOException;

    @Override
    public boolean reachedEnd() {
        return !iterator.hasNext();
    }

    public void setOriginalJob(List<Map<String, Object>> originalJob){
        this.originalJob = originalJob;
    }

}
