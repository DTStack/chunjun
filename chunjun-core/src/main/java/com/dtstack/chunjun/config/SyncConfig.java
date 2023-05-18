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
package com.dtstack.chunjun.config;

import com.dtstack.chunjun.cdc.CdcConfig;
import com.dtstack.chunjun.mapping.MappingConfig;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.util.Preconditions;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@ToString
@Getter
@Setter
public class SyncConfig implements Serializable {

    private static final long serialVersionUID = 1964981610706788313L;

    /** ChunJun job */
    private JobConfig job;

    /** ChunJun提交端的插件包路径 */
    private String pluginRoot;

    /** ChunJun运行时服务器上的远程端插件包路径 */
    private String remotePluginPath;

    private String savePointPath;

    /** 本次任务所需插件jar包路径列表 */
    private List<String> syncJarList;

    private String mode;

    public static SyncConfig parseJob(String jobJson) {
        SyncConfig config = GsonUtil.GSON.fromJson(jobJson, SyncConfig.class);
        checkJob(config);
        return config;
    }

    private static void checkJob(SyncConfig config) {
        List<ContentConfig> content = config.getJob().getContent();

        Preconditions.checkNotNull(
                content,
                "[content] in the task script is empty, please check the task script configuration.");
        Preconditions.checkArgument(
                content.size() != 0,
                "[content] in the task script is empty, please check the task script configuration.");

        // 检查reader配置
        OperatorConfig reader = config.getReader();
        Preconditions.checkNotNull(
                reader,
                "[reader] in the task script is empty, please check the task script configuration.");
        String readerName = reader.getName();
        Preconditions.checkNotNull(
                readerName,
                "[name] under [reader] in task script is empty, please check task script configuration.");
        Map<String, Object> readerParameter = reader.getParameter();
        Preconditions.checkNotNull(
                readerParameter,
                "[parameter] under [reader] in the task script is empty, please check the configuration of the task script.");

        // 检查writer配置
        OperatorConfig writer = config.getWriter();
        Preconditions.checkNotNull(
                writer,
                "[writer] in the task script is empty, please check the task script configuration.");
        String writerName = writer.getName();
        Preconditions.checkNotNull(
                writerName,
                "[name] under [writer] in the task script is empty, please check the configuration of the task script.");
        Map<String, Object> writerParameter = writer.getParameter();
        Preconditions.checkNotNull(
                writerParameter,
                "[parameter] under [writer] in the task script is empty, please check the configuration of the task script.");

        List<FieldConfig> readerFieldList = config.getReader().getFieldList();
        // 检查并设置restore
        RestoreConfig restore = config.getRestore();
        if (restore.isStream()) {
            // 实时任务restore必须设置为true，用于数据ck恢复
            restore.setRestore(true);
        } else if (restore.isRestore()) { // 离线任务开启断点续传
            FieldConfig fieldColumnByName =
                    FieldConfig.getSameNameMetaColumn(
                            readerFieldList, restore.getRestoreColumnName());
            FieldConfig fieldColumnByIndex = null;
            if (restore.getRestoreColumnIndex() >= 0) {
                fieldColumnByIndex = readerFieldList.get(restore.getRestoreColumnIndex());
            }

            FieldConfig fieldColumn;
            boolean columnWithoutName =
                    readerFieldList.stream().noneMatch(i -> StringUtils.isNotBlank(i.getName()));
            // 如果column没有name 且restoreColumnIndex为-1 则不需要校验
            if (fieldColumnByName == null
                    && columnWithoutName
                    && restore.getRestoreColumnIndex() == -1) {
                return;
            } else if (fieldColumnByName == null && fieldColumnByIndex == null) {
                throw new IllegalArgumentException(
                        "Can not find restore column from json with column name:"
                                + restore.getRestoreColumnName());
            } else if (fieldColumnByName != null
                    && fieldColumnByIndex != null
                    && fieldColumnByName != fieldColumnByIndex) {
                throw new IllegalArgumentException(
                        String.format(
                                "The column name and column index point to different columns, column name = [%s]，point to [%s]; column index = [%s], point to [%s].",
                                restore.getRestoreColumnName(),
                                fieldColumnByName,
                                restore.getRestoreColumnIndex(),
                                fieldColumnByIndex));
            } else {
                fieldColumn = fieldColumnByName != null ? fieldColumnByName : fieldColumnByIndex;
            }

            restore.setRestoreColumnIndex(fieldColumn.getIndex());
            restore.setRestoreColumnType(fieldColumn.getType().getType());
        }
    }

    public OperatorConfig getReader() {
        return job.getReader();
    }

    public OperatorConfig getWriter() {
        return job.getWriter();
    }

    public TransformerConfig getTransformer() {
        return job.getTransformer();
    }

    public SpeedConfig getSpeed() {
        return job.getSetting().getSpeed();
    }

    public RestoreConfig getRestore() {
        return job.getSetting().getRestore();
    }

    public MetricPluginConfig getMetricPluginConf() {
        return job.getSetting().getMetricPluginConfig();
    }

    public CdcConfig getCdcConf() {
        return job.getCdcConf();
    }

    public MappingConfig getNameMappingConfig() {
        return job.getNameMapping();
    }
}
