/**
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

package com.dtstack.flinkx.launcher;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;

/**
 * The Parser of Launcher commandline options
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class LauncherOptionParser {

    public static final String OPTION_MODE = "mode";

    public static final String OPTION_JOB = "job";

    public static final String OPTION_MONITOR = "monitor";

    public static final String OPTION_JOB_ID = "jobid";

    public static final String OPTION_FLINK_CONF_DIR = "flinkconf";

    public static final String OPTION_PLUGIN_ROOT = "plugin";

    public static final String OPTION_YARN_CONF_DIR = "yarnconf";

    public static final String OPTION_QUEUE ="queue";

    public static final String OPTION_FLINK_LIB_JAR = "flinkLibJar";

    private Options options = new Options();

    private BasicParser parser = new BasicParser();

    private LauncherOptions launcherOptions = new LauncherOptions();

    public LauncherOptionParser(String[] args) {
        options.addOption(OPTION_MODE, true, "Running mode");
        options.addOption(OPTION_MONITOR, true, "Monitor url of flink cluster");
        options.addOption(OPTION_JOB, true, "Job description json file");
        options.addOption(OPTION_FLINK_CONF_DIR, true, "Flink configuration directory");
        options.addOption(OPTION_PLUGIN_ROOT, true, "FlinkX plugin root");
        options.addOption(OPTION_YARN_CONF_DIR, true, "Yarn and hadoop configuration directory");
        options.addOption(OPTION_QUEUE, true, "yarn job queue");
        options.addOption(OPTION_FLINK_LIB_JAR, true, "flink lib jar path");
        try {
            CommandLine cl = parser.parse(options, args);

            String mode = cl.getOptionValue(OPTION_MODE, ClusterMode.local.name());
            launcherOptions.setMode(mode);

            String jobId = cl.getOptionValue(OPTION_JOB_ID, "default_job_id");
            launcherOptions.setJobid(jobId);

            String job = Preconditions.checkNotNull(cl.getOptionValue(OPTION_JOB),
                    "Must specify job file using option '" + OPTION_JOB + "'");
            File file = new File(job);
            FileInputStream in = new FileInputStream(file);
            byte[] filecontent = new byte[(int) file.length()];
            in.read(filecontent);
            String content = new String(filecontent, "UTF-8");
            launcherOptions.setJob(content);

            String pluginRoot = Preconditions.checkNotNull(cl.getOptionValue(OPTION_PLUGIN_ROOT));
            launcherOptions.setPlugin(pluginRoot);

            String flinkConfDir = cl.getOptionValue(OPTION_FLINK_CONF_DIR);
            if(StringUtils.isNotBlank(flinkConfDir)) {
                launcherOptions.setFlinkconf(flinkConfDir);
            }

            String yarnConfDir = cl.getOptionValue(OPTION_YARN_CONF_DIR);
            if(StringUtils.isNotBlank(yarnConfDir)) {
                launcherOptions.setYarnconf(yarnConfDir);
            }
            launcherOptions.setQueue(cl.getOptionValue(OPTION_QUEUE,"default"));
            launcherOptions.setFlinkLibJar(cl.getOptionValue(OPTION_FLINK_LIB_JAR));
        } catch (Exception e) {
            printUsage();
            throw new RuntimeException(e);
        }
    }

    public LauncherOptions getLauncherOptions(){
        return launcherOptions;
    }

    private void printUsage() {
        System.out.print(options.toString());
    }

}
