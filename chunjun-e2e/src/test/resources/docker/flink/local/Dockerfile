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

