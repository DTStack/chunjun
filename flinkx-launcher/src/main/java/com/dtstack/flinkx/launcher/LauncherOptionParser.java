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
import java.util.Properties;
import static com.dtstack.flinkx.launcher.ClusterMode.*;
import static com.dtstack.flinkx.launcher.LauncherOptions.*;

/**
 * The Parser of Launcher commandline options
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class LauncherOptionParser {

    private static final String DEFAULT_JOB_ID = "default_job_id";

    private Options options = new Options();

    private BasicParser parser = new BasicParser();

    private Properties properties = new Properties();

    public LauncherOptionParser(String[] args) {
        options.addOption(LauncherOptions.OPTION_MODE, true, "Running mode");
        options.addOption(OPTION_MONITOR, true, "Monitor url of flink cluster");
        options.addOption(OPTION_JOB, true, "Job description json file");
        options.addOption(OPTION_FLINK_CONF_DIR, true, "Flink configuration directory");
        options.addOption(OPTION_PLUGIN_ROOT, true, "FlinkX plugin root");
        options.addOption(OPTION_YARN_CONF_DIR, true, "Yarn and hadoop configuration directory");

        try {
            CommandLine cl = parser.parse(options, args);

            String mode = cl.getOptionValue(OPTION_MODE, MODE_LOCAL);
            properties.put(OPTION_MODE, mode);

            String jobId = cl.getOptionValue(OPTION_JOB_ID, DEFAULT_JOB_ID);
            properties.put(OPTION_JOB_ID, jobId);

            String job = Preconditions.checkNotNull(cl.getOptionValue(OPTION_JOB),
                    "Must specify job file using option '" + OPTION_JOB + "'");
            File file = new File(job);
            FileInputStream in = new FileInputStream(file);
            byte[] filecontent = new byte[(int) file.length()];
            in.read(filecontent);
            String content = new String(filecontent, "UTF-8");
            properties.put(OPTION_JOB, content);

            String pluginRoot = Preconditions.checkNotNull(cl.getOptionValue(OPTION_PLUGIN_ROOT));
            properties.put(OPTION_PLUGIN_ROOT, pluginRoot);

            String flinkConfDir = cl.getOptionValue(OPTION_FLINK_CONF_DIR);
            if(StringUtils.isNotBlank(flinkConfDir)) {
                properties.put(OPTION_FLINK_CONF_DIR, flinkConfDir);
            }

            String yarnConfDir = cl.getOptionValue(OPTION_YARN_CONF_DIR);
            if(StringUtils.isNotBlank(yarnConfDir)) {
                properties.put(OPTION_YARN_CONF_DIR, yarnConfDir);
            }

        } catch (Exception e) {
            printUsage();
            throw new RuntimeException(e);
        }

    }

    public Properties getProperties(){
        return properties;
    }

    private void printUsage() {

    }

}
