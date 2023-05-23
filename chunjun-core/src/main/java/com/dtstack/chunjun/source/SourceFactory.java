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

package com.dtstack.chunjun.source;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SpeedConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.RawTypeConvertible;
import com.dtstack.chunjun.util.PropertiesUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public abstract class SourceFactory implements RawTypeConvertible {

    protected StreamExecutionEnvironment env;
    protected SyncConfig syncConfig;
    protected List<FieldConfig> fieldList;
    protected TypeInformation<RowData> typeInformation;
    protected boolean useAbstractBaseColumn = true;

    protected SourceFactory(SyncConfig syncConfig, StreamExecutionEnvironment env) {
        this.env = env;
        this.syncConfig = syncConfig;
        List<FieldConfig> readerFiledConfList = syncConfig.getReader().getFieldList();
        this.fieldList = new ArrayList<>(readerFiledConfList.size());
        fieldList.addAll(readerFiledConfList);
        if (syncConfig.getTransformer() != null
                && !StringUtils.isBlank(syncConfig.getTransformer().getTransformSql())) {
            useAbstractBaseColumn = false;
        } else {
            fieldList.forEach(
                    fieldConfig -> {
                        /*
                         In a synchronous job, we don't care about the name field when
                         typeInformation is generated.To avoid name duplication/empty, use
                         indexValue
                        */
                        if (fieldConfig.getName() == null) {
                            fieldConfig.setName(String.valueOf(fieldConfig.getIndex()));
                        }
                    });
        }
    }

    /**
     * Build the read data flow object
     *
     * @return DataStream
     */
    public abstract DataStream<RowData> createSource();

    /**
     * 同步任务使用transform。不支持*、不支持常量、不支持format、必须是flinksql支持的类型 常量和format都可以在transform中做。
     *
     * @param commonConfig
     */
    protected void checkConstant(CommonConfig commonConfig) {
        List<FieldConfig> fieldList = commonConfig.getColumn();
        if (fieldList.size() == 1
                && StringUtils.equals(ConstantValue.STAR_SYMBOL, fieldList.get(0).getName())) {
            com.google.common.base.Preconditions.checkArgument(
                    false, "in transformer mode : not support '*' in column.");
        }
        commonConfig.getColumn().stream()
                .forEach(
                        x -> {
                            if (StringUtils.isNotBlank(x.getValue())) {
                                com.google.common.base.Preconditions.checkArgument(
                                        false,
                                        "in transformer mode : not support default value,you can set value in transformer");
                            }
                            if (StringUtils.isNotBlank(x.getFormat())) {
                                com.google.common.base.Preconditions.checkArgument(
                                        false,
                                        "in transformer mode : not support default format,you can set format in transformer");
                            }
                        });
    }

    protected DataStream<RowData> createInput(
            InputFormat<RowData, InputSplit> inputFormat, String sourceName) {
        Preconditions.checkNotNull(sourceName);
        Preconditions.checkNotNull(inputFormat);
        DtInputFormatSourceFunction<RowData> function =
                new DtInputFormatSourceFunction<>(inputFormat, getTypeInformation());
        return env.addSource(function, sourceName, getTypeInformation());
    }

    protected DataStream<RowData> createInput(
            RichParallelSourceFunction<RowData> function, String sourceName) {
        Preconditions.checkNotNull(sourceName);
        return env.addSource(function, sourceName);
    }

    protected DataStream<RowData> createInput(InputFormat<RowData, InputSplit> inputFormat) {
        return createInput(inputFormat, this.getClass().getSimpleName().toLowerCase());
    }

    /** 初始化CommonConfig */
    public void initCommonConf(CommonConfig commonConfig) {
        PropertiesUtil.initCommonConf(commonConfig, this.syncConfig);
        commonConfig.setCheckFormat(this.syncConfig.getReader().getBooleanVal("check", true));
        SpeedConfig speed = this.syncConfig.getSpeed();
        commonConfig.setParallelism(
                speed.getReaderChannel() == -1 ? speed.getChannel() : speed.getReaderChannel());
    }

    protected TypeInformation<RowData> getTypeInformation() {
        if (typeInformation == null) {
            typeInformation =
                    TableUtil.getTypeInformation(
                            fieldList, getRawTypeMapper(), useAbstractBaseColumn);
        }
        return typeInformation;
    }
}
