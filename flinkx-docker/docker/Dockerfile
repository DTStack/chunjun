###############################################################################
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
###############################################################################

FROM openjdk:8-jdk

# Install dependencies
RUN set -ex; \
  apt-get update; \
  apt-get -y install libsnappy1v5 gettext-base libjemalloc-dev vim; \
  rm -rf /var/lib/apt/lists/*

# Grab gosu for easy step-down from root
COPY gosu /usr/local/bin/
RUN set -ex; \
  gosu nobody true

# Prepare environment
ENV FLINK_HOME=/opt/flink
ENV PATH=$FLINK_HOME/bin:$PATH
RUN groupadd --system --gid=9999 flink && \
    useradd --system --home-dir $FLINK_HOME --uid=9999 --gid=flink flink
WORKDIR $FLINK_HOME

# Copy Flinkx Plugins
ENV FLINKX_HOME=/opt/flinkx-dist
COPY flinkx-dist $FLINKX_HOME
RUN chown -R flink:flink $FLINKX_HOME

# Install Arthas
#ENV ARTHAS_HOME=/opt/arthas
#RUN mkdir $ARTHAS_HOME; \
#    wget -nv -O $ARTHAS_HOME/arthas.zip https://arthas.aliyun.com/download/3.5.1?mirror=center; \
#    cd $ARTHAS_HOME; \
#    unzip arthas.zip; \
#    rm arthas.zip

# Install Flink
# Already downloaded flink.tgz
#COPY flink.tgz $FLINK_HOME
#RUN set -ex; \
#    cd $FLINK_HOME; \
#    tar -xf flink.tgz --strip-components=1; \
#    rm flink.tgz; \
#    chown -R flink:flink .; \

# Did not download flink.tgz
RUN set -ex; \
  cd $FLINK_HOME; \
  wget -nv -O flink.tgz https://archive.apache.org/dist/flink/flink-1.12.7/flink-1.12.7-bin-scala_2.12.tgz; \
  tar -xf flink.tgz --strip-components=1; \
  rm flink.tgz; \
  chown -R flink:flink .;

# Configure container
COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
EXPOSE 6123 8081
CMD ["help"]
