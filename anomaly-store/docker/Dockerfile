FROM openjdk:8-jre

ENV APP_NAME anomaly-store
ENV APP_HOME /app/bin
ENV JMXTRANS_AGENT jmxtrans-agent-1.2.6
ENV DOCKERIZE_VERSION v0.6.1

ADD https://github.com/jwilder/dockerize/releases/download/${DOCKERIZE_VERSION}/dockerize-alpine-linux-amd64-${DOCKERIZE_VERSION}.tar.gz dockerize.tar.gz
RUN tar xzf dockerize.tar.gz
RUN chmod +x dockerize

RUN mkdir -p ${APP_HOME}
RUN mkdir -p ${APP_HOME}/storage-backends

COPY target/haystack-anomaly-store.jar ${APP_HOME}/anomaly-store.jar
COPY storage-backends ${APP_HOME}/storage-backends
COPY docker/start-app.sh ${APP_HOME}/
COPY docker/jmxtrans-agent.xml ${APP_HOME}/

ADD https://github.com/jmxtrans/jmxtrans-agent/releases/download/${JMXTRANS_AGENT}/${JMXTRANS_AGENT}.jar ${APP_HOME}/

WORKDIR ${APP_HOME}

ENTRYPOINT ["./start-app.sh"]
