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
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.Charsets;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The Parser of Launcher commandline options
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class OptionParser {

    private final static String OPTION_JOB = "job";

    private org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();

    private BasicParser parser = new BasicParser();

    private Options properties = new Options();

    public OptionParser(String[] args) throws Exception {
        initOptions(addOptions(args));
    }

    private CommandLine addOptions(String[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, ParseException {
        Class cla = properties.getClass();
        Field[] fields = cla.getDeclaredFields();
        for(Field field:fields){
            String name = field.getName();
            OptionRequired optionRequired = field.getAnnotation(OptionRequired.class);
            if(optionRequired != null){
                options.addOption(name,optionRequired.hasArg(),optionRequired.description());
            }
        }
        CommandLine cl = parser.parse(options, args);
        return cl;
    }

    private void initOptions(CommandLine cl) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, ParseException {
        Class cla = properties.getClass();
        Field[] fields = cla.getDeclaredFields();
        for(Field field:fields){
            String name = field.getName();
            String value = cl.getOptionValue(name);
            OptionRequired optionRequired = field.getAnnotation(OptionRequired.class);
            if(optionRequired != null){
                if(optionRequired.required()&&StringUtils.isBlank(value)){
                    throw new RuntimeException(String.format("parameters of %s is required",name));
                }
            }
            if(StringUtils.isNotBlank(value)){
                field.setAccessible(true);
                field.set(properties,value);
            }
        }
    }

    public Options getOptions(){
        return properties;
    }

    public List<String> getProgramExeArgList() throws Exception {
        Map<String,Object> mapConf = MapUtil.objectToMap(properties);
        List<String> args = new ArrayList<>();
        for(Map.Entry<String, Object> one : mapConf.entrySet()){
            String key = one.getKey();
            Object value = one.getValue();
            if(value == null){
                continue;
            }else if(OPTION_JOB.equalsIgnoreCase(key)){
                File file = new File(value.toString());
                FileInputStream in = new FileInputStream(file);
                byte[] filecontent = new byte[(int) file.length()];
                in.read(filecontent);
                value = new String(filecontent, Charsets.UTF_8.name());
            }
            args.add("-" + key);
            args.add(value.toString());
        }
        return args;
    }

    private void printUsage() {
        System.out.print(options.toString());
    }

}
