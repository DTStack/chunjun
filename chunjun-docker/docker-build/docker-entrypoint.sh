#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


FILE="/opt/flink/lib/chunjun-dist/chunjun-examples/json/stream/stream.json"
JOBTYPE="sync"
MODE="standalone"
files=$(ls /opt/flink/job)
for file in $files; do
  FILE="/opt/flink/job/$file"
  echo "$FILE"
  if [[ $FILE == *.sql ]];
    then JOBTYPE="sql"
  fi
done

while getopts ":m:" opt
do
  case $opt in
    m)
      MODE=$OPTARG;;
    ?)
      echo "getopts param error"
  esac
done

echo "mode is $MODE"
echo "job file is $FILE"
echo "job type is $JOBTYPE"
if [[ $MODE == standalone ]]
  then bash /opt/flink/bin/start-cluster.sh
fi

/opt/flink/lib/chunjun-dist/bin/start-chunjun -mode $MODE -jobType $JOBTYPE -chunjunDistDir /opt/flink/lib/chunjun-dist  -job $FILE -flinkConfDir $FLINK_HOME/conf
if [[ $MODE == local ]];
  then exit
fi
while true; do
  echo "default web monitor URL is localhost:8081, using the real port if you defined in standalone mode."
  sleep 90
done

