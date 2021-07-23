FROM repository-us-west-2.teo.dev.ascint.sabrecirrus.com:9084/openjdk/openjdk-11-rhel7







COPY target/et-finalization-service-0.0.1-SNAPSHOT.jar /deployments

#unzip installed profile agent in direcotry
RUN mkdir -p /opt/cprof && \
  wget -q -O- https://storage.googleapis.com/cloud-profiler/java/latest/profiler_java_agent.tar.gz \
  | tar xzv -C /opt/cprof

#wget -q -O- https://storage.googleapis.com/cloud-profiler/java/latest/profiler_java_agent_alpine.tar.gz \
#| tar xzv -C /opt/cprof

#to list all the available versions
#gsutil ls gs://cloud-profiler/java/cloud-profiler-*

#to download specific version of profiler agent
#wget -q -O- https://storage.googleapis.com/cloud-profiler/java/cloud-profiler-java-agent_20191028_RC00.tar.gz \
#  | sudo tar xzv -C /opt/cprof

#To enable logging
java -agentpath:/opt/cprof/profiler_java_agent.so=-cprof_service=et-finalization-service,
-logtostderr,-minloglevel=2 \ -jar /deployments/et-finalization-service-0.0.1-SNAPSHOT.jar

EXPOSE 8080 8050

# redhat-openjdk-18/openjdk18-openshift uses ./run-java.sh script as entrypoint.
# This general purpose startup script is optimized for running Java application from within containers. By default it takes a jar file from /deployments directory.
# Full reference: https://access.redhat.com/documentation/en-us/red_hat_jboss_middleware_for_openshift/3/html/red_hat_java_s2i_for_openshift/reference
