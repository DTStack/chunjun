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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The class used for generating data-transfer-task json files using variable substitution
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class ExampleGenerator {
    private static final String OPTION_CONF_DIR = "c";
    private static final String OPTION_TEMPLATE_DIR = "t";
    private static final String OPTION_OUTPUT_DIR = "o";
    private Properties substituteMap = new Properties();
    private String confDir;
    private String templateDir;
    private String outputDir;
    private List<String> tempList = new ArrayList<>();

    public ExampleGenerator(String confDir, String templateDir, String outputDir) {
        this.confDir = confDir;
        this.templateDir = templateDir;
        this.outputDir = outputDir;
    }

    public void generate() throws IOException {
        File dir = new File(outputDir);
        if(!dir.exists()) {
            dir.mkdir();
        } else if(!dir.isDirectory()) {
            dir.delete();
            dir.mkdir();
        }

        initVarMap();

        initTempList();

        for(String tempFile : tempList) {
            String[] part = tempFile.split(File.separator);
            String outputPath = outputDir + File.separator + part[part.length - 1];
            try(BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(tempFile)))) {
                try(BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath)))) {
                    String line;
                    while((line = br.readLine()) != null) {
                        bw.write(substituteVars(line));
                        bw.write("\n");
                    }
                }
            }
        }

    }

    public void initTempList() throws IOException {
        File dir = new File(templateDir);
        if(dir.exists() && dir.isDirectory()) {
            File[] tempFiles = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".json");
                }
            });
            if(tempFiles != null) {
                for(File tempFile : tempFiles) {
                    tempList.add(tempFile.getPath());
                }
            }
        }
    }

    public void initVarMap() throws IOException {
        File dir = new File(confDir);
        if(dir.exists() && dir.isDirectory()) {
            File[] confFiles = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".conf");
                }
            });
            for(File confFile : confFiles) {
                substituteMap.load(new FileInputStream(confFile));
            }
        }
    }

    public  String substituteVars(String str) {
        StringBuilder sb = new StringBuilder();
        String pattern = "\\$\\{(.+?)\\}";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(str);
        int start = 0;
        while(m.find()) {
            String var = m.group(1);
            if(substituteMap.containsKey(var)) {
                sb.append(str.substring(start, m.start()));
                sb.append(substituteMap.get(var));
                start = m.end();
            }
        }
        if(start < str.length()) {
            sb.append(str.substring(start));
        }
        return sb.toString();
    }

    private static ExampleGenerator getInstance(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(OPTION_CONF_DIR, true, "Variable configuration directory");
        options.addOption(OPTION_TEMPLATE_DIR, true, "Task template directory");
        options.addOption(OPTION_OUTPUT_DIR, true, "Output Directory");
        BasicParser parser = new BasicParser();
        CommandLine cmdLine = parser.parse(options, args);
        String confDir = cmdLine.getOptionValue(OPTION_CONF_DIR);
        String templateDir = cmdLine.getOptionValue(OPTION_TEMPLATE_DIR);
        String outputDir = cmdLine.getOptionValue(OPTION_OUTPUT_DIR);
        return new ExampleGenerator(confDir, templateDir, outputDir);
    }

    public static void main(String[] args) throws IOException, ParseException {
        ExampleGenerator generator = getInstance(args);
        generator.generate();
    }

}
