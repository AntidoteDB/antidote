FROM erlang:21

LABEL maintainer="ilyas@toumlilt.com"

ENV HANDOFF_PORT "8099"
ENV PB_PORT "8087"
ENV PB_IP "0.0.0.0"
ENV PBSUB_PORT "8086"
ENV LOGREADER_PORT "8085"
ENV RING_STATE_DIR "data/ring"
ENV PLATFORM_DATA_DIR "data"
ENV NODE_NAME "antidote@127.0.0.1"
ENV SHORT_NAME "false"

ENV SRC_SCRIPTS_DIR "./Dockerfiles"
ENV ANTIDOTE_BUILD_DIR "/usr/src/antidote"
ENV ANTIDOTE_RUNTIME_DIR "/opt"

COPY . $ANTIDOTE_BUILD_DIR

RUN set -xe \
    && apt-get update \
    && apt-get install -y --no-install-recommends git openssl ca-certificates \
    && cd $ANTIDOTE_BUILD_DIR \
    && make rel \
    && cp -R ${ANTIDOTE_BUILD_DIR}/_build/default/rel/antidote $ANTIDOTE_RUNTIME_DIR/ \
    && sed -e '$i,{kernel, [{inet_dist_listen_min, 9100}, {inet_dist_listen_max, 9100}]}' ${ANTIDOTE_BUILD_DIR}/_build/default/rel/antidote/releases/0.0.2/sys.config > ${ANTIDOTE_RUNTIME_DIR}/antidote/releases/0.0.2/sys.config \
    && rm -rf ${ANTIDOTE_BUILD_DIR} /var/lib/apt/lists/*

ADD ${SRC_SCRIPTS_DIR}/start_and_attach.sh ${ANTIDOTE_RUNTIME_DIR}/antidote/
ADD ${SRC_SCRIPTS_DIR}/entrypoint.sh /

RUN chmod a+x ${ANTIDOTE_RUNTIME_DIR}/antidote/start_and_attach.sh \
    && chmod a+x /entrypoint.sh

# Distributed Erlang Port Mapper
EXPOSE 4369
# Ports for Antidote
EXPOSE 8085 8086 8087 8099

# Antidote RPC
EXPOSE 9100

VOLUME ${ANTIDOTE_RUNTIME_DIR}/antidote/data

ENTRYPOINT ["/entrypoint.sh"]

CMD ["sh", "-c", "${ANTIDOTE_RUNTIME_DIR}/antidote/start_and_attach.sh"]
