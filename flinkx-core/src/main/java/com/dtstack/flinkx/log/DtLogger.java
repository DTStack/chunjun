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
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.filter.ThresholdFilter;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy;
import ch.qos.logback.core.util.FileSize;
import ch.qos.logback.core.util.OptionHelper;
import com.dtstack.flinkx.config.LogConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.varia.LevelRangeFilter;
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

    public static final String APPEND_NAME = "flinkx";
    public static final String LOGGER_NAME = "com.dtstack";
    public static final String LOG4J = "org.slf4j.impl.Log4jLoggerFactory";
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
                if (LOG4J.equalsIgnoreCase(type)) {
                    configLog4j(logConfig, jobId);
                } else if (LOGBACK.equalsIgnoreCase(type)) {
                    configLogback(logConfig, jobId);
                }

                init = true;
            }
        }
    }

    private static void configLog4j(LogConfig logConfig, String jobId) {
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(LOGGER_NAME);
        org.apache.log4j.Level level = org.apache.log4j.Level.toLevel(logConfig.getLevel());
        LEVEL_INT = level.toInt();
        String pattern = logConfig.getPattern();
        String path = logConfig.getPath();

        logger.removeAllAppenders();
        logger.setAdditivity(true);
        logger.setLevel(level);

        org.apache.log4j.RollingFileAppender appender = new org.apache.log4j.RollingFileAppender();
        PatternLayout layout = new PatternLayout();
        if (StringUtils.isNotBlank(pattern)) {
            layout.setConversionPattern(pattern);
        } else {
            layout.setConversionPattern(LogConfig.DEFAULT_LOG4J_PATTERN);
        }

        LevelRangeFilter filter = new LevelRangeFilter();
        filter.setLevelMin(level);
        appender.addFilter(filter);
        appender.setLayout(layout);
        appender.setFile(path + jobId + ".log");
        appender.setEncoding(StandardCharsets.UTF_8.name());
        appender.setMaxFileSize("1GB");
        appender.setMaxBackupIndex(1);
        appender.setAppend(true);
        appender.activateOptions();
        appender.setName(APPEND_NAME);

        logger.removeAllAppenders();
        logger.addAppender(appender);

        logger.info("DtLogger config successfully, current log is [log4j]");
    }

    @SuppressWarnings("unchecked")
    private static void configLogback(LogConfig logConfig, String jobId) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger logger = context.getLogger(LOGGER_NAME);

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

        logger.info("DtLogger config successfully, current log is [logback]");
    }

    public static boolean isEnableTrace(){
        return Level.TRACE_INT >= LEVEL_INT;
    }

    public static boolean isEnableDebug(){
        return Level.DEBUG_INT >= LEVEL_INT;
    }
}
