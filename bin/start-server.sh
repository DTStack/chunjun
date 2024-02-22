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

#set -x

export CHUNJUN_HOME="$(cd "`dirname "$0"`"/..; pwd)"
echo "CHUNJUN_HOME:"$CHUNJUN_HOME >&2

cd $CHUNJUN_HOME
HO_HEAP_SIZE="${HO_HEAP_SIZE:=1024m}"

JAVA_OPTS="$JAVA_OPTS -Xmx${HO_HEAP_SIZE}"
#JAVA_OPTS="$JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=10006"

JAVA_OPTS="$JAVA_OPTS -Xms${HO_HEAP_SIZE}"

JAVA_OPTS="$JAVA_OPTS -server"

JAVA_OPTS="$JAVA_OPTS -Xloggc:../logs/node.gc"

JAVA_OPTS="$JAVA_OPTS -XX:HeapDumpPath=../logs/heapdump.hprof"

JAVA_OPTS="$JAVA_OPTS -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+HeapDumpOnOutOfMemoryError -XX:+DisableExplicitGC -Dfile.encoding=UTF-8 -Djna.nosys=true -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps"

#JAVA_OPTS="$JAVA_OPTS -Djava.io.tmpdir=./tmpSave"
CHUNJUN_NICE=19
CHUNJUN_LOG_DIR=$CHUNJUN_HOME/logs
pidfile=$CHUNJUN_HOME/run/chunjun.pid

if [ ! -d "$CHUNJUN_LOG_DIR" ]; then
	mkdir $CHUNJUN_LOG_DIR
fi

if [ ! -e "$pidfile" ]; then
	mkdir $CHUNJUN_HOME/run
fi



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

if [ -n "$HADOOP_USER_NAME" ]; then
	echo "HADOOP_USER_NAME is "$HADOOP_USER_NAME
else
	HADOOP_USER_NAME=yarn
	echo "set HADOOP_USER_NAME as yarn"
	export HADOOP_USER_NAME
fi

JAR_DIR=$CHUNJUN_HOME/server/*:$CHUNJUN_HOME/*.jar:$CHUNJUN_HOME/conf/*
echo $JAR_DIR
CLASS_NAME=com.dtstack.chunjun.server.ServerLauncher

start(){
    echo "ChunJun server starting ..."
	nice -n ${CHUNJUN_NICE} $JAVA_RUN  $JAVA_OPTS -cp $JAR_DIR $CLASS_NAME $@ 1> "${CHUNJUN_LOG_DIR}/chunjun.stdout" 2>&1  &
    echo $! > $pidfile
	ret=$?
	return 0
}

status() {
  if [ -f "$pidfile" ] ; then
    pid=`cat "$pidfile"`
    if kill -0 $pid > /dev/null 2> /dev/null ; then
      # process by this pid is running.
      # It may not be our pid, but that's what you get with just pidfiles.
      # TODO(sissel): Check if this process seems to be the same as the one we
      # expect. It'd be nice to use flock here, but flock uses fork, not exec,
      # so it makes it quite awkward to use in this case.
      return 0
    else
      return 2 # program is dead but pid file exists
    fi
  else
    return 3
  fi
}


stop() {
  echo -n "Stoping chunjun server "
  # Try a few times to kill TERM the program
  if status ; then
    pid=`cat "$pidfile"`
    echo "Killing chunjun  (pid $pid) with SIGTERM"
    kill -TERM $pid
    # Wait for it to exit.
    for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do
      echo "Waiting chunjun  (pid $pid) to die..."
      status || break
      sleep 1
    done
    if status ; then
      if [ "$KILL_ON_STOP_TIMEOUT" == "1" ] ; then
        echo "Timeout reached. Killing chunjun (pid $pid) with SIGKILL. This may result in data loss."
        kill -KILL $pid
        echo "chunjun killed with SIGKILL."
      else
        echo "chunjun stop failed; still running."
        return 1 # stop timed out and not forced
      fi
    else
      echo -n "chunjun stopped "
    fi
  fi
  ret=$?
  [ $ret -eq 0 ] ; echo "stop success"

}

case "$1" in
	start)
		echo "start"
		status
		code=$?
        if [ $code -eq 0 ]; then
			echo "chunjun server is already running"
		else
			start
			code=$?
       fi
	   exit $code
	   ;;
    status)
		status
	    code=$?
		if [ $code -eq 0 ];then
			echo "chunjun is already running"
		else
			echo "chunjun is not running"
		fi
		exit $code
        ;;
    stop)
        stop
		;;
	*)
	   echo "!!! only support 'start' 'status' 'stop' operator."
esac
exit $?
