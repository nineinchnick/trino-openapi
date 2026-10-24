ARG TRINO_VERSION
FROM trinodb/trino-core:$TRINO_VERSION AS plugin

ARG VERSION

COPY target/trino-openapi-$VERSION.zip /tmp/trino-openapi.zip
RUN mkdir /tmp/trino-openapi && \
    cd /tmp/trino-openapi && \
    jar --extract --file /tmp/trino-openapi.zip

FROM trinodb/trino-core:$TRINO_VERSION

ARG VERSION

COPY --chown=trino:trino --from=plugin /tmp/trino-openapi/trino-openapi-$VERSION/ /usr/lib/trino/plugin/openapi/
ADD catalog/ /etc/trino/catalog/disabled/
ADD specs/ /etc/trino/specs/
ADD docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["/usr/lib/trino/bin/launcher", "run", "--etc-dir", "/etc/trino"]
