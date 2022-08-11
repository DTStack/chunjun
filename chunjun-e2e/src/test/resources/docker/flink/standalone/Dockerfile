FROM openjdk:8-jdk-alpine

ENV TZ=Asia/Shanghai
ENV LANG=C.UTF-8

RUN apk add --no-cache bash

RUN wget https://mirrors.cloud.tencent.com/apache/flink/flink-1.12.7/flink-1.12.7-bin-scala_2.12.tgz && \
    tar -zxvf flink-1.12.7-bin-scala_2.12.tgz && \
    rm flink-1.12.7-bin-scala_2.12.tgz && \
    mv flink-1.12.7 /opt/flink

COPY ./flink-conf.yaml /opt/flink/conf
COPY ./slf4j-log4j12-1.7.10.jar /opt/flink/lib
COPY entrypoint.sh /opt/flink

EXPOSE 6123
EXPOSE 8081

WORKDIR /opt/flink
ENTRYPOINT ["bash","entrypoint.sh"]

