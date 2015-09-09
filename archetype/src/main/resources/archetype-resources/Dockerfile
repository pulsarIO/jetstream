FROM java:openjdk-7-jdk

# Change the project_version value when artifact version changed.
ENV project_version ${version}
ENV project_name ${artifactId}

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

#Suggested production JAVA_OPS for Open JDK.
#ENV JETSTREAM_JAVA_OPTS -server -Xms6g -Xmx6g -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseCompressedOops -X:MaxTenuringThreshold=8 -XX:CMSInitiatingOccupancyFraction=75 -XX:MaxNewSize=3g -XX:NewSize=3g -XX:+CMSConcurrentMTEnabled -XX:+CMSScavengeBeforeRemark 

# Set the default port info
ENV JETSTREAM_REST_BASEPORT 8080
ENV JETSTREAM_CONTEXT_BASEPORT 15590
ENV JETSTREAM_APP_PORT 9999

EXPOSE 9999 15590 15591 15592 15593 15594 15595 15596 15597 15598 15599
ENTRYPOINT ./start.sh
