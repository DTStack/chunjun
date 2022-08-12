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

export CHUNJUN_HOME="$(cd "`dirname "$0"`"/..; pwd)"
echo "CHUNJUN_HOME is $CHUNJUN_HOME"

# exit if CHUNJUN_HOME is not set or not a directory
if [ -z $CHUNJUN_HOME ] || [ ! -d $CHUNJUN_HOME ];then
    echo "failed to build docker image because CHUNJUN_HOME is not set"
    exit 1
fi
cd $CHUNJUN_HOME/docker-build
echo "start building a docker image named chunjun-master"
tar -czvf chunjun-dist.tar.gz -C $CHUNJUN_HOME .
docker build -t chunjun-master .
