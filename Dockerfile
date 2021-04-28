FROM adoptopenjdk/openjdk11:alpine-jre

# maintainer info
LABEL maintainer="aarfi.siddique@infogain.com"

# add volume pointing to /tmp
VOLUME /tmp

# Make port 9000 available to the world outside the container
EXPOSE 9000

# application jar file when packaged
ARG jar_file=target/pnr-resequencer-service.jar

# add application jar file to container
COPY ${jar_file} pnr-resequencer-service.jar

# run the jar file
ENTRYPOINT ["java", "-jar", "pnr-resequencer-service.jar"]