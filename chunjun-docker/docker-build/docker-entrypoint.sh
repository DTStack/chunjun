#!/usr/bin/env bash

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

