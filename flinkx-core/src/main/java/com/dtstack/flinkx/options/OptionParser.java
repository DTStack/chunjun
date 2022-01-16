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
package com.dtstack.flinkx.options;

import com.dtstack.flinkx.util.MapUtil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Date: 2021/03/18 Company: www.dtstack.com
 *
 * @author tudou
 */
public class OptionParser {

    private static final String OPTION_JOB = "job";

    private final org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();

    private final DefaultParser parser = new DefaultParser();

    private final Options properties = new Options();

    public OptionParser(String[] args) throws Exception {
        Class cla = properties.getClass();
        Field[] fields = cla.getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            OptionRequired optionRequired = field.getAnnotation(OptionRequired.class);
            if (optionRequired != null) {
                options.addOption(name, optionRequired.hasArg(), optionRequired.description());
            }
        }
        CommandLine cl = parser.parse(options, args);

        for (Field field : fields) {
            String name = field.getName();
            String value = cl.getOptionValue(name);
            OptionRequired optionRequired = field.getAnnotation(OptionRequired.class);

            if (optionRequired != null) {
                if (optionRequired.required() && StringUtils.isBlank(value)) {
                    throw new RuntimeException(String.format("parameters of %s is required", name));
                }
            }

            if (StringUtils.isNotBlank(value)) {
                field.setAccessible(true);
                field.set(properties, value);
            }
        }
    }

    public List<String> getProgramExeArgList() throws Exception {
        Map<String, Object> mapConf = MapUtil.objectToMap(properties);
        List<String> args = new ArrayList<>();
        for (Map.Entry<String, Object> one : mapConf.entrySet()) {
            String key = one.getKey();
            Object value = one.getValue();
            if (value == null) {
                continue;
            } else if (OPTION_JOB.equalsIgnoreCase(key)) {
                File file = new File(value.toString());
                try (FileInputStream in = new FileInputStream(file)) {
                    byte[] fileContent = new byte[(int) file.length()];
                    in.read(fileContent);
                    value =
                            URLEncoder.encode(
                                    new String(fileContent, StandardCharsets.UTF_8),
                                    StandardCharsets.UTF_8.name());
                }
            }
            args.add("-" + key);
            args.add(value.toString());
        }
        return args;
    }

    public Options getOptions() {
        return properties;
    }
}
