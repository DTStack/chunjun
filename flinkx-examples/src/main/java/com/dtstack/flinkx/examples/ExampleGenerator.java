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

package com.dtstack.flinkx.examples;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import java.io.IOException;
import java.util.Properties;

/**
 * The class used for generating data-transfer-task json files using variable substitution
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class ExampleGenerator {
    private static final String OPTION_CONF_DIR = "c";
    private static final String OPTION_TEMPLATE_DIR = "t";
    private static final String DEFAULT_CONF_DIR;
    private static final String DEFAULT_TEMPLATE_DIR;
    private Properties substituteMap = new Properties();
    private String confDir;
    private String templateDir;

    public ExampleGenerator(String confDir, String templateDir) {
        this.confDir = confDir;
        this.templateDir = templateDir;
    }

    static {
        DEFAULT_CONF_DIR = "";
        DEFAULT_TEMPLATE_DIR = "";
    }

    public void generate() throws IOException {
        initVarMap();

    }

    private void initVarMap() {

    }

    private String substituteVars(String str) {
        return null;
    }

    private static ExampleGenerator getInstance(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(OPTION_CONF_DIR, true, "Variable configuration directory");
        options.addOption(OPTION_TEMPLATE_DIR, true, "Task template directory");
        BasicParser parser = new BasicParser();
        CommandLine cmdLine = parser.parse(options, args);
        String confDir = cmdLine.getOptionValue(OPTION_CONF_DIR, DEFAULT_CONF_DIR);
        String templateDir = cmdLine.getOptionValue(OPTION_CONF_DIR, DEFAULT_TEMPLATE_DIR);
        return new ExampleGenerator(confDir,templateDir);
    }

    public static void main(String[] args) throws IOException, ParseException {
        ExampleGenerator generator = getInstance(args);
        generator.generate();
    }
}
