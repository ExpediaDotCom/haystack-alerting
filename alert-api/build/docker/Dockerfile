FROM openjdk:8-jre
MAINTAINER Haystack <haystack@expedia.com>

ENV APP_NAME haystack-alert-api
ENV APP_HOME /app/bin
ENV JMXTRANS_AGENT jmxtrans-agent-1.2.6

RUN mkdir -p ${APP_HOME}
RUN mkdir -p ${APP_HOME}/storage-backends

COPY target/${APP_NAME}.jar ${APP_HOME}/
COPY storage-backends ${APP_HOME}/storage-backends
COPY build/docker/start-app.sh ${APP_HOME}/

RUN GRPC_HEALTH_PROBE_VERSION=v0.2.0 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe


COPY build/docker/jmxtrans-agent.xml ${APP_HOME}/
ADD https://github.com/jmxtrans/jmxtrans-agent/releases/download/${JMXTRANS_AGENT}/${JMXTRANS_AGENT}.jar ${APP_HOME}/

WORKDIR ${APP_HOME}

ENTRYPOINT ["./start-app.sh"]
