FROM apache/flink:1.12.7-scala_2.12-java8
COPY chunjun-dist.tar.gz /opt/flink/lib/
RUN mkdir /opt/flink/lib/chunjun-dist | tar -zxvf /opt/flink/lib/chunjun-dist.tar.gz -C /opt/flink/lib/chunjun-dist
ENV CHUNJUN_HOME /opt/flink/lib/chunjun-dist
COPY docker-entrypoint.sh /
RUN chmod 777 /docker-entrypoint.sh
