#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  JAVA_RUN="${JAVA_HOME}/bin/java"
else
  if [ `command -v java` ]; then
    JAVA_RUN="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

#1: deploy with assembly dist package file
#2: deploy with project package
CHUNJUN_DEPLOY_MODE=1
if [[ $CHUNJUN_HOME && -z $CHUNJUN_HOME ]];then
  export CHUNJUN_HOME=$CHUNJUN_HOME
else
  CHUNJUN_HOME="$(cd "`dirname "$0"`"/..; pwd)"
  if [ -d "$CHUNJUN_HOME/chunjun-dist" ]; then
    CHUNJUN_HOME="$CHUNJUN_HOME/chunjun-dist"
    CHUNJUN_DEPLOY_MODE=2
  fi
fi
# 1.In yarn-session case, JAR_DIR can not be found
# 2.In other cases, JAR_DIR can be found
if [ $CHUNJUN_DEPLOY_MODE -eq 1 ]; then
  JAR_DIR=$CHUNJUN_HOME/lib/chunjun-clients.jar:$CHUNJUN_HOME/lib/*
else
  JAR_DIR=$CHUNJUN_HOME/../lib/chunjun-clients.jar:$CHUNJUN_HOME/../lib/*
fi

CLASS_NAME=com.dtstack.chunjun.client.Launcher

JOBTYPE="sync"
ARGS=$@
if [[ $ARGS == *.sql* ]];
  then JOBTYPE="sql"
fi;

echo "
          #                               #
          #                               #
          #
  #####   ######   #     #  # ####     ####   #     #  # ####
 #        #     #  #     #  ##    #       #   #     #  ##    #
 #        #     #  #     #  #     #       #   #     #  #     #
 #        #     #  #    ##  #     #       #   #    ##  #     #
  #####   #     #   #### #  #     #       #    #### #  #     #
                                          #
                                      ####

Reference site: https://dtstack.github.io/chunjun

chunjun is starting ...
CHUNJUN_HOME is auto set $CHUNJUN_HOME"

# basic parameters for all jobs
PARAMS="$ARGS -mode $MODE -jobType $JOBTYPE -chunjunDistDir $CHUNJUN_HOME"

# if FLINK_HOME is not set or not a directory, ignore flinkConfDir parameter
if [ ! -z $FLINK_HOME ] && [ -d $FLINK_HOME ];
  then
    echo "FLINK_HOME is $FLINK_HOME"
    PARAMS="$PARAMS -flinkConfDir $FLINK_HOME/conf -flinkLibDir $FLINK_HOME/lib"
  else
    echo "FLINK_HOME is empty!"
fi

# if HADOOP_HOME is not set or not a directory, ignore hadoopConfDir parameter
if [ ! -z $HADOOP_HOME ] && [ -d $HADOOP_HOME ];
  then
    echo "HADOOP_HOME is $HADOOP_HOME"
    PARAMS="$PARAMS -hadoopConfDir $HADOOP_HOME/etc/hadoop"
  else
    echo "HADOOP_HOME is empty!"
fi

# add a new line to separate from the log
echo ""
echo "start command: $JAVA_RUN -cp $JAR_DIR $CLASS_NAME $PARAMS"
echo ""

$JAVA_RUN -cp $JAR_DIR $CLASS_NAME $PARAMS
