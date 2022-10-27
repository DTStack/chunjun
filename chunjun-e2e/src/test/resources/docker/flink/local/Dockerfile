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

FROM ubuntu:20.04

ENV SHELL /bin/bash
ENV LANG=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

#1,install jdk
RUN apt-get update \
    && apt-get -y install openjdk-8-jdk

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

#install wget
RUN apt-get -y install wget curl

RUN wget https://mirrors.cloud.tencent.com/apache/flink/flink-1.12.7/flink-1.12.7-bin-scala_2.12.tgz && \
    tar -zxvf flink-1.12.7-bin-scala_2.12.tgz && \
    rm flink-1.12.7-bin-scala_2.12.tgz && \
    mv flink-1.12.7 /opt/flink

COPY ../docker/flink/standalone/flink-conf.yaml /opt/flink/conf
COPY ../docker/flink/standalone/slf4j-log4j12-1.7.10.jar /opt/flink/lib
COPY ../docker/flink/standalone/entrypoint.sh /opt/flink

EXPOSE 6123
EXPOSE 8081

WORKDIR /opt/flink
ENTRYPOINT ["bash","entrypoint.sh"]

