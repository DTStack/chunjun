/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.source;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.streaming.api.functions.source.DtInputFormatSourceFunction;
import com.dtstack.flinkx.util.PropertiesUtil;
import com.dtstack.flinkx.util.TableUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;

/**
 * Abstract specification of Reader Plugin
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class BaseDataSource {

    protected StreamExecutionEnvironment env;
    protected SyncConf syncConf;
    protected TypeInformation<Row> typeInformation;

    protected BaseDataSource(SyncConf syncConf, StreamExecutionEnvironment env) {
        this.env = env;
        initColumn(syncConf);
        this.syncConf = syncConf;

        if(syncConf.getTransformer() == null || StringUtils.isBlank(syncConf.getTransformer().getTransformSql())){
            typeInformation = TableUtil.getRowTypeInformation(Collections.emptyList());
        }else{
            typeInformation = TableUtil.getRowTypeInformation(syncConf.getReader().getFieldList());
        }
    }

    /**
     * Build the read data flow object
     *
     * @return DataStream
     */
    public abstract DataStream<Row> readData();

    @SuppressWarnings("unchecked")
    protected DataStream<Row> createInput(InputFormat inputFormat, String sourceName) {
        Preconditions.checkNotNull(sourceName);
        Preconditions.checkNotNull(inputFormat);
//        TypeInformation typeInfo = TypeExtractor.getInputFormatTypes(inputFormat);
        DtInputFormatSourceFunction function = new DtInputFormatSourceFunction(inputFormat, typeInformation);
        return env.addSource(function, sourceName, typeInformation);
    }

    protected DataStream<Row> createInput(RichParallelSourceFunction<Row> function, String sourceName) {
        Preconditions.checkNotNull(sourceName);
        return env.addSource(function, sourceName, typeInformation);
    }

    protected DataStream<Row> createInput(InputFormat inputFormat) {
        return createInput(inputFormat, this.getClass().getSimpleName().toLowerCase());
    }

    /**
     *
     * getMetaColumns(columns, true); 默认对column里index为空时处理为对应数据在数组里的下标而不是-1
     * 如果index为-1是有特殊逻辑 需要覆盖此方法使用 getMetaColumns(List columns, false) 代替
     * @param config 配置信息
     */
    protected void initColumn(SyncConf config){
        List<MetaColumn> readerColumnList = MetaColumn.getMetaColumns(config.getReader().getMetaColumn());
        if(CollectionUtils.isNotEmpty(readerColumnList)){
            config.getReader().getParameter().put(ConfigConstant.KEY_COLUMN, readerColumnList);
        }
    }

    /**
     * 初始化FlinkxCommonConf
     * @param flinkxCommonConf
     */
    public void initFlinkxCommonConf(FlinkxCommonConf flinkxCommonConf){
        PropertiesUtil.initFlinkxCommonConf(flinkxCommonConf, this.syncConf);
        flinkxCommonConf.setCheckFormat(this.syncConf.getReader().getBooleanVal("check", true));
    }

    public SyncConf getSyncConf() {
        return syncConf;
    }

    public void setSyncConf(SyncConf syncConf) {
        this.syncConf = syncConf;
    }
}
