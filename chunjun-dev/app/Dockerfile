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

FROM archlinux:base-devel

WORKDIR /tmp
ENV SHELL /bin/bash
ADD mirrorlist /etc/pacman.d/mirrorlist
RUN yes | pacman -Syu
RUN yes | pacman -S git zsh
VOLUME [ "/root/", "/root/.local/share/chezmoi", "/root/.vscode-server/extensions", "/var/lib/docker" ]
# end

# bash
ADD bashrc /root/.bashrc
# end

# basic tools
RUN yes | pacman -S curl tree wget vim vi
ENV EDITOR=vim
ENV VISUAL=vim
# end

# tools
RUN yes | pacman -S fzf openssh exa the_silver_searcher fd rsync  \
    && ssh-keygen -t rsa -N '' -f /etc/ssh/ssh_host_rsa_key &&\
		ssh-keygen -t dsa -N '' -f /etc/ssh/ssh_host_dsa_key

RUN echo "root:root" | chpasswd

# Java
RUN yes | pacman -S jdk8-openjdk
ENV JAVA_HOME=/usr/lib/jvm/default
ENV PATH=$JAVA_HOME/bin:$PATH

# maven
ENV MAVEN_VERSION 3.8.6
ENV MAVEN_HOME=/usr/lib/mvn
ENV PATH=$MAVEN_HOME/bin:$PATH

RUN wget http://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz && \
  tar -zxvf apache-maven-$MAVEN_VERSION-bin.tar.gz && \
  rm apache-maven-$MAVEN_VERSION-bin.tar.gz && \
  mv apache-maven-$MAVEN_VERSION /usr/lib/mvn

COPY ./settings.xml /usr/lib/mvn/conf
RUN mkdir -p /root/.m2
RUN mkdir -p /root/.m2/repository
COPY ./settings.xml /root/.m2/
COPY ./clean_maven.sh /root

RUN mkdir -p /root/app/code
WORKDIR /root/app/code

COPY ./sshd_config /etc/ssh

EXPOSE 22

CMD ["/usr/sbin/sshd", "-D"]
