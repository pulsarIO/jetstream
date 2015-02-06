FROM java:openjdk-7-jdk

MAINTAINER Xinglang Wang <wangxinglang@gmail.com>

ENV project_version 3.2.1-SNAPSHOT
ENV project_name subscriber

COPY target/${project_name}-${project_version}-bin.tar.gz /opt/app/${project_name}-${project_version}-bin.tar.gz
WORKDIR /opt/app
RUN tar -zxvf ${project_name}-${project_version}-bin.tar.gz
RUN ln -s /opt/app/${project_name}-${project_version} jetstreamapp
WORKDIR /opt/app/jetstreamapp

# App config
ENV JETSTREAM_APP_JAR_NAME ${project_name}.jar
ENV JETSTREAM_APP_NAME ${project_name}
ENV JETSTREAM_CONFIG_VERSION 1.0

# Dependency
ENV JETSTREAM_ZKSERVER_HOST zkserver
ENV JETSTREAM_ZKSERVER_PORT 2181
ENV JETSTREAM_MONGOURL mongo://mongoserver:27017/config

EXPOSE 9999 15590 15591 15592 15593 15594 15595 15596 15597 15598 15599
ENTRYPOINT ./start.sh
