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
package com.dtstack.flinkx.log;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.filter.ThresholdFilter;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy;
import ch.qos.logback.core.util.FileSize;
import ch.qos.logback.core.util.OptionHelper;
import com.dtstack.flinkx.config.LogConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.filter.LevelRangeFilter;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.spi.StandardLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * Date: 2019/12/18
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class DtLogger {
    private static Logger LOG = LoggerFactory.getLogger(DtLogger.class);
    private static boolean init = false;
    public static final String LOG4J = "org.apache.logging.slf4j.Log4jLoggerFactory";

    public static final String APPEND_NAME = "flinkx";
    public static final String LOGGER_NAME = "com.dtstack";
    private static boolean isLog4j2;
    public static final String LOGBACK = "ch.qos.logback.classic.util.ContextSelectorStaticBinder";
    public static int LEVEL_INT = Integer.MAX_VALUE;

    public static void config(LogConfig logConfig, String jobId) {
        if (logConfig == null || !logConfig.isLogger() || init) {
            return;
        }
        synchronized (DtLogger.class) {
            if (!init) {
                String path = logConfig.getPath();
                File file = new File(path);
                if (!file.exists() && !file.mkdirs()) {
                    LOG.warn("cannot create directory [{}]", path);
                    return;
                }

                String type = StaticLoggerBinder.getSingleton().getLoggerFactoryClassStr();
                LOG.info("current log type is {}", type);
                if (LOG4J.equalsIgnoreCase(type)) {
                    configLog4j(logConfig, jobId);
                    isLog4j2 = true;
                } else if (LOGBACK.equalsIgnoreCase(type)) {
                    configLogback(logConfig, jobId);
                    isLog4j2 = false;
                }else{
                    LOG.warn("log type {} is not [org.apache.logging.slf4j.Log4jLoggerFactory], either nor [ch.qos.logback.classic.util.ContextSelectorStaticBinder]", type);
                }

                init = true;
            }
        }
     }

    private static void configLog4j(LogConfig logConfig, String jobId) {
        LOG.info("start to config log4j...");
        LoggerContext loggerContext = (LoggerContext) LogManager.getContext();
        Configuration config = loggerContext.getConfiguration();

        org.apache.logging.log4j.Level level = org.apache.logging.log4j.Level.toLevel(logConfig.getLevel());
        LEVEL_INT = level.intLevel();
        String pattern = logConfig.getPattern();
        String path = logConfig.getPath();

        if (StringUtils.isBlank(pattern)) {
            pattern = LogConfig.DEFAULT_LOG4J_PATTERN;
        }

        PatternLayout layout = PatternLayout.newBuilder()
                .withCharset(StandardCharsets.UTF_8)
                .withConfiguration(config)
                .withPattern(pattern)
                .build();

        Filter filter = LevelRangeFilter.createFilter(org.apache.logging.log4j.Level.ERROR,
                level,
                Filter.Result.ACCEPT,
                Filter.Result.DENY);

        Appender appender = org.apache.logging.log4j.core.appender.RollingFileAppender.newBuilder()
                .withAppend(true)
//                .setFilter(filter)
                .withFileName(path + File.separator + jobId + ".log")
                .withFilePattern(path + File.separator + jobId + ".%i.log")
                .setName(APPEND_NAME)
                .withPolicy(SizeBasedTriggeringPolicy.createPolicy("1GB"))
                .setLayout(layout)
                .setConfiguration(config)
                .build();
        appender.start();

        for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.addAppender(appender, level, filter);
            loggerConfig.setAdditive(false);
            loggerConfig.setLevel(level);
        }

        LOG.info("DtLogger config successfully, current log is [log4j]");
    }

    @SuppressWarnings("unchecked")
    private static void configLogback(LogConfig logConfig, String jobId) {
        LOG.info("start to config logback...");
        final ch.qos.logback.classic.LoggerContext context = (ch.qos.logback.classic.LoggerContext) LoggerFactory.getILoggerFactory();
        final ch.qos.logback.classic.Logger logger = context.getLogger(LOGGER_NAME);

        Level level = Level.toLevel(logConfig.getLevel());
        LEVEL_INT = level.toInt();
        String pattern = logConfig.getPattern();
        String path = logConfig.getPath();
        RollingFileAppender appender = new RollingFileAppender();

        ThresholdFilter filter = new ThresholdFilter();
        filter.setLevel(level.toString());
        filter.start();
        appender.addFilter(filter);
        appender.setContext(context);
        appender.setName(APPEND_NAME);

        appender.setFile(OptionHelper.substVars(path + jobId + ".log", context));
        appender.setAppend(true);
        appender.setPrudent(false);
        SizeAndTimeBasedRollingPolicy policy = new SizeAndTimeBasedRollingPolicy();
        String fp = OptionHelper.substVars(path+ jobId + "/.%d{yyyy-MM-dd}.%i.log",context);
        policy.setMaxFileSize("1GB");
        policy.setFileNamePattern(fp);
        policy.setMaxHistory(15);
        policy.setTotalSizeCap(FileSize.valueOf("1GB"));
        policy.setParent(appender);
        policy.setContext(context);
        policy.start();

        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        if (StringUtils.isNotBlank(pattern)) {
            encoder.setPattern(pattern);
        } else {
            encoder.setPattern(LogConfig.DEFAULT_LOGBACK_PATTERN);
        }
        encoder.start();

        appender.setRollingPolicy(policy);
        appender.setEncoder(encoder);
        appender.start();
        ch.qos.logback.classic.Logger root = context.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        root.setLevel(level);


        logger.setLevel(level);
        logger.setAdditive(true);
        logger.addAppender(appender);

        LOG.info("DtLogger config successfully, current log is [logback]");
    }

    public static boolean isEnableTrace(){
        if(isLog4j2){
            return StandardLevel.TRACE.intLevel() >= LEVEL_INT;
        }else{
            return Level.TRACE.levelInt >= LEVEL_INT;
        }
    }

    public static boolean isEnableDebug(){
        if(isLog4j2){
            return StandardLevel.DEBUG.intLevel() >= LEVEL_INT;
        }else{
            return Level.DEBUG.levelInt >= LEVEL_INT;
        }
    }
}
