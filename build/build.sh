#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

find_build=`find build -type f -maxdepth 2`
if [[ $find_build == "" ]] ; then
	echo "You have to call the script from the chunjun/ dir"
	exit 1
fi

# find pluginLibs path
CMD_PATH=`dirname $0`
CMD_HOME=`cd "$CMD_PATH"/../; pwd`
PLUGINS_DIR=$CMD_HOME/syncplugins

help() {
	usage='usage: build.sh [<Options>]\nbuild with module \nAction: \n  -h, help info\n  -m, build modules'
	echo -e $usage
}

# parse args
is_clear="false"
modules=""

while getopts "m:ch" opt; do
  case $opt in
    m)
      modules=$OPTARG
      ;;
    c)
      is_clear="true"
      ;;
    h)
      help
      exit 0
      ;;
    \?)
      echo "Invalid option"
      exit 1
      ;;
  esac
done

# clear pluginLibs
if [[ "true" == "$is_clear" && -d $PLUGINS_DIR ]]; then
  echo "Clear dir: $PLUGINS_DIR"
  rm -rf $PLUGINS_DIR
fi

# build module
if [[ -z $modules ]]; then
  mvn -T 1C clean package -DskipTests
else
  echo "build modules: $modules"
  # TODO get vaild module path
  vaild_module_path=$modules
  echo "build module paths: $vaild_module_path"
  mvn -T 1C clean package -DskipTests -pl $vaild_module_path -am
fi



