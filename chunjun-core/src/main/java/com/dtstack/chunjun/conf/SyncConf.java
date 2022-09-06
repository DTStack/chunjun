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
package com.dtstack.chunjun.conf;

import com.dtstack.chunjun.cdc.CdcConf;
import com.dtstack.chunjun.mapping.NameMappingConf;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Date: 2021/01/18 Company: www.dtstack.com
 *
 * @author tudou
 */
public class SyncConf implements Serializable {
    private static final long serialVersionUID = 1L;

    /** ChunJun job */
    private JobConf job;

    /** ChunJun提交端的插件包路径 */
    private String pluginRoot;
    /** ChunJun运行时服务器上的远程端插件包路径 */
    private String remotePluginPath;

    private String savePointPath;

    /** 本次任务所需插件jar包路径列表 */
    private List<String> syncJarList;

    /**
     * 解析job字符串
     *
     * @param jobJson job json字符串
     * @return ChunJunJobConfig
     */
    public static SyncConf parseJob(String jobJson) {
        SyncConf config = GsonUtil.GSON.fromJson(jobJson, SyncConf.class);
        checkJob(config);
        return config;
    }

    /**
     * 校验Job配置
     *
     * @param config ChunJunJobConfig
     */
    private static void checkJob(SyncConf config) {
        List<ContentConf> content = config.getJob().getContent();

        Preconditions.checkNotNull(
                content,
                "[content] in the task script is empty, please check the task script configuration.");
        Preconditions.checkArgument(
                content.size() != 0,
                "[content] in the task script is empty, please check the task script configuration.");

        // 检查reader配置
        OperatorConf reader = config.getReader();
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
        OperatorConf writer = config.getWriter();
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

        List<FieldConf> readerFieldList = config.getReader().getFieldList();
        // 检查并设置restore
        RestoreConf restore = config.getRestore();
        if (restore.isStream()) {
            // 实时任务restore必须设置为true，用于数据ck恢复
            restore.setRestore(true);
        } else if (restore.isRestore()) { // 离线任务开启断点续传
            FieldConf fieldColumnByName =
                    FieldConf.getSameNameMetaColumn(
                            readerFieldList, restore.getRestoreColumnName());
            FieldConf fieldColumnByIndex = null;
            if (restore.getRestoreColumnIndex() >= 0) {
                fieldColumnByIndex = readerFieldList.get(restore.getRestoreColumnIndex());
            }

            FieldConf fieldColumn;
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
            restore.setRestoreColumnType(fieldColumn.getType());
        }
    }

    public OperatorConf getReader() {
        return job.getReader();
    }

    public OperatorConf getWriter() {
        return job.getWriter();
    }

    public TransformerConf getTransformer() {
        return job.getTransformer();
    }

    public SpeedConf getSpeed() {
        return job.getSetting().getSpeed();
    }

    public LogConf getLog() {
        return job.getSetting().getLog();
    }

    public RestartConf getRestart() {
        return job.getSetting().getRestart();
    }

    public RestoreConf getRestore() {
        return job.getSetting().getRestore();
    }

    public JobConf getJob() {
        return job;
    }

    public void setJob(JobConf job) {
        this.job = job;
    }

    public String getPluginRoot() {
        return pluginRoot;
    }

    public void setPluginRoot(String pluginRoot) {
        this.pluginRoot = pluginRoot;
    }

    public String getRemotePluginPath() {
        return remotePluginPath;
    }

    public void setRemotePluginPath(String remotePluginPath) {
        this.remotePluginPath = remotePluginPath;
    }

    public String getSavePointPath() {
        return savePointPath;
    }

    public void setSavePointPath(String savePointPath) {
        this.savePointPath = savePointPath;
    }

    public MetricPluginConf getMetricPluginConf() {
        return job.getSetting().getMetricPluginConf();
    }

    public CdcConf getCdcConf() {
        return job.getCdcConf();
    }

    public List<String> getSyncJarList() {
        return syncJarList;
    }

    public void setSyncJarList(List<String> syncJarList) {
        this.syncJarList = syncJarList;
    }

    public NameMappingConf getNameMappingConf() {
        return job.getNameMapping();
    }

    @Override
    public String toString() {
        return "SyncConf{"
                + "job="
                + job
                + ", pluginRoot='"
                + pluginRoot
                + '\''
                + ", remotePluginPath='"
                + remotePluginPath
                + '\''
                + ", savePointPath='"
                + savePointPath
                + '\''
                + ", syncJarList="
                + syncJarList
                + '}';
    }

    /**
     * 转换成字符串，不带job脚本内容
     *
     * @return
     */
    public String asString() {
        return "SyncConf{"
                + "pluginRoot='"
                + pluginRoot
                + '\''
                + ", remotePluginPath='"
                + remotePluginPath
                + '\''
                + ", savePointPath='"
                + savePointPath
                + '\''
                + ", syncJarList="
                + syncJarList
                + '}';
    }
}
