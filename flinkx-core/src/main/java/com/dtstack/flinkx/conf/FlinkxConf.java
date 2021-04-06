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
package com.dtstack.flinkx.conf;

import com.dtstack.flinkx.source.MetaColumn;
import com.dtstack.flinkx.util.GsonUtil;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Date: 2021/01/18
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class FlinkxConf implements Serializable {
    private static final long serialVersionUID = 1L;

    /** FlinkX job */
    private JobConf job;

    /** FlinkX jobId */
    private String jobId;
    /** FlinkX on yarn时的monitorUrl */
    private String monitorUrls;
    /** FlinkX提交端的插件包路径 */
    private String pluginRoot;
    /** FlinkX运行时服务器上的远程端插件包路径 */
    private String remotePluginPath;

    /**
     * 解析job字符串
     * @param jobJson job json字符串
     * @return FlinkxJobConfig
     */
    public static FlinkxConf parseJob(String jobJson){
        FlinkxConf config = GsonUtil.GSON.fromJson(jobJson, FlinkxConf.class);
        checkJob(config);
        return config;
    }

    /**
     * 校验Job配置
     * @param config FlinkxJobConfig
     */
    private static void checkJob(FlinkxConf config) {
        List<ContentConf> content = config.getJob().getContent();

        Preconditions.checkNotNull(content, "[content] in the task script is empty, please check the task script configuration.");
        Preconditions.checkArgument(content.size() != 0, "[content] in the task script is empty, please check the task script configuration.");

        // 检查reader配置
        SourceConf reader = config.getReader();
        Preconditions.checkNotNull(reader, "[reader] in the task script is empty, please check the task script configuration.");
        String readerName = reader.getName();
        Preconditions.checkNotNull(readerName, "[name] under [reader] in task script is empty, please check task script configuration.");
        Map<String, Object> readerParameter = reader.getParameter();
        Preconditions.checkNotNull(readerParameter, "[parameter] under [reader] in the task script is empty, please check the configuration of the task script.");


        // 检查writer配置
        SinkConf writer = config.getWriter();
        Preconditions.checkNotNull(writer, "[writer] in the task script is empty, please check the task script configuration.");
        String writerName = writer.getName();
        Preconditions.checkNotNull(writerName, "[name] under [writer] in the task script is empty, please check the configuration of the task script.");
        Map<String, Object> writerParameter = reader.getParameter();
        Preconditions.checkNotNull(writerParameter, "[parameter] under [writer] in the task script is empty, please check the configuration of the task script.");

        List<MetaColumn> readerColumnList = MetaColumn.getMetaColumns(config.getReader().getMetaColumn());
        //检查并设置restore
        RestoreConf restore = config.getRestore();
        if(restore.isStream()){
            //实时任务restore必须设置为true，用于数据ck恢复
            restore.setRestore(true);
        }else if(restore.isRestore()){  //离线任务开启断点续传
            MetaColumn metaColumnByName = MetaColumn.getSameNameMetaColumn(readerColumnList, restore.getRestoreColumnName());
            MetaColumn metaColumnByIndex = null;
            if(restore.getRestoreColumnIndex() >= 0){
                metaColumnByIndex = readerColumnList.get(restore.getRestoreColumnIndex());
            }

            MetaColumn metaColumn;

            if(metaColumnByName == null && metaColumnByIndex == null){
                throw new IllegalArgumentException("Can not find restore column from json with column name:" + restore.getRestoreColumnName());
            }else if(metaColumnByName != null && metaColumnByIndex != null && metaColumnByName != metaColumnByIndex){
                throw new IllegalArgumentException(String.format("The column name and column index point to different columns, column name = [%s]，point to [%s]; column index = [%s], point to [%s].",
                        restore.getRestoreColumnName(), metaColumnByName, restore.getRestoreColumnIndex(), metaColumnByIndex));
            }else{
                metaColumn = metaColumnByName != null ? metaColumnByName : metaColumnByIndex;
            }

            restore.setRestoreColumnIndex(metaColumn.getIndex());
            restore.setRestoreColumnType(metaColumn.getType());
        }
    }

    public SourceConf getReader(){
        return job.getReader();
    }

    public SinkConf getWriter(){
        return job.getWriter();
    }

    public SpeedConf getSpeed(){
        return job.getSetting().getSpeed();
    }

    public DirtyConf getDirty(){
        return job.getSetting().getDirty();
    }

    public ErrorLimitConf getErrorLimit(){
        return job.getSetting().getErrorLimit();
    }

    public LogConf getLog(){
        return job.getSetting().getLog();
    }

    public RestartConf getRestart(){
        return job.getSetting().getRestart();
    }

    public RestoreConf getRestore(){
        return job.getSetting().getRestore();
    }

    public JobConf getJob() {
        return job;
    }

    public void setJob(JobConf job) {
        this.job = job;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getMonitorUrls() {
        return monitorUrls;
    }

    public void setMonitorUrls(String monitorUrls) {
        this.monitorUrls = monitorUrls;
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

    @Override
    public String toString() {
        return "FlinkxConf{" +
                "job=" + job +
                ", jobId='" + jobId + '\'' +
                ", monitorUrls='" + monitorUrls + '\'' +
                ", pluginRoot='" + pluginRoot + '\'' +
                ", remotePluginPath='" + remotePluginPath + '\'' +
                '}';
    }

    /**
     * 转换成字符串，不带job脚本内容
     * @return
     */
    public String asString() {
        return "FlinkxConf{" +
                "jobId='" + jobId + '\'' +
                ", monitorUrls='" + monitorUrls + '\'' +
                ", pluginRoot='" + pluginRoot + '\'' +
                ", remotePluginPath='" + remotePluginPath + '\'' +
                '}';
    }
}
