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

package com.dtstack.flinkx.exec;

import com.dtstack.flinkx.enums.ClusterMode;
import com.dtstack.flinkx.enums.EPluginLoadMode;
import com.dtstack.flinkx.options.OptionParser;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.util.PluginUtil;
import com.dtstack.flinkx.util.SampleUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.dtstack.flinkx.constants.ConfigConstant.SAMPLE_INTERVAL_COUNT;

/**
 * 任务执行时的流程方法
 * Date: 2020/2/17
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class ExecuteProcessHelper {

    private static final String CLASS_FILE_NAME_FMT = "class_path_%d";
    private static final Logger LOG = LoggerFactory.getLogger(ExecuteProcessHelper.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @SuppressWarnings("unchecked")
    public static ParamsInfo parseParams(String[] args) throws Exception {
        LOG.info("------------program params-------------------------");
        Arrays.stream(args).forEach(arg -> LOG.info("{}", arg));
        LOG.info("-------------------------------------------");

        OptionParser optionParser = new OptionParser(args);
        Options options = optionParser.getOptions();

        String sql = URLDecoder.decode(options.getJob(), Charsets.UTF_8.name());
        String name = options.getJobName();
        String localSqlPluginPath = options.getPluginRoot();
        String remoteSqlPluginPath = options.getRemotePluginPath();
        String pluginLoadMode = options.getPluginLoadMode();
        String deployMode = options.getMode();
        String connectorLoadMode = options.getConnectorLoadMode();

        Preconditions.checkArgument(checkRemoteSqlPluginPath(remoteSqlPluginPath, deployMode, pluginLoadMode),
                "Non-local mode or shipfile deployment mode, remoteSqlPluginPath is required");
        String confProp = URLDecoder.decode(options.getConfProp(), Charsets.UTF_8.toString());
        Properties confProperties = PluginUtil.jsonStrToObject(confProp, Properties.class);

        List<URL> jarUrlList = getExternalJarUrls(options.getAddjar());

        return ParamsInfo.builder()
                .setSql(sql)
                .setName(name)
                .setLocalSqlPluginPath(localSqlPluginPath)
                .setRemoteSqlPluginPath(remoteSqlPluginPath)
                .setPluginLoadMode(pluginLoadMode)
                .setDeployMode(deployMode)
                .setConfProp(confProperties)
                .setJarUrlList(jarUrlList)
                .setConnectorLoadMode(connectorLoadMode)
                .build();

    }

    /**
     * 非local模式或者shipfile部署模式，remoteSqlPluginPath必填
     *
     * @param remoteSqlPluginPath
     * @param deployMode
     * @param pluginLoadMode
     * @return
     */
    public static boolean checkRemoteSqlPluginPath(String remoteSqlPluginPath, String deployMode, String pluginLoadMode) {
        if (StringUtils.isEmpty(remoteSqlPluginPath)) {
            return StringUtils.equalsIgnoreCase(pluginLoadMode, EPluginLoadMode.SHIPFILE.name())
                    || StringUtils.equalsIgnoreCase(deployMode, ClusterMode.local.name());
        }
        return true;
    }

    private static void setSamplingIntervalCount(ParamsInfo paramsInfo) {
        SampleUtils.setSamplingIntervalCount(
                Integer.parseInt(
                        paramsInfo.getConfProp().getProperty(SAMPLE_INTERVAL_COUNT, "0")
                )
        );
    }

    public static List<URL> getExternalJarUrls(String addJarListStr) throws java.io.IOException {
        List<URL> jarUrlList = Lists.newArrayList();
        if (Strings.isNullOrEmpty(addJarListStr)) {
            return jarUrlList;
        }

        List<String> addJarFileList = OBJECT_MAPPER.readValue(URLDecoder.decode(addJarListStr, Charsets.UTF_8.name()), List.class);
        //Get External jar to load
        for (String addJarPath : addJarFileList) {
            jarUrlList.add(new File(addJarPath).toURI().toURL());
        }
        return jarUrlList;
    }

    /**
     * perjob模式将job依赖的插件包路径存储到cacheFile，在外围将插件包路径传递给jobgraph
     *
     * @param env
     * @param classPathSet
     */
    public static void registerPluginUrlToCachedFile(StreamExecutionEnvironment env, Set<URL> classPathSet) {
        int i = 0;
        for (URL url : classPathSet) {
            String classFileName = String.format(CLASS_FILE_NAME_FMT, i);
            env.registerCachedFile(url.getPath(), classFileName, true);
            i++;
        }
    }
}