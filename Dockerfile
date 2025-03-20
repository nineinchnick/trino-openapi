ARG TRINO_VERSION
FROM trinodb/trino-core:$TRINO_VERSION

ARG VERSION

ADD target/trino-openapi-$VERSION/ /usr/lib/trino/plugin/openapi/
ADD catalog/ /etc/trino/catalog/disabled/
ADD docker-entrypoint.sh /usr/local/bin/

ENV OPENAPI_AUTH_TYPE=none \
    OPENAPI_TOKEN_ENDPOINT=/oauth/token \
    OPENAPI_CLIENT_ID="" \
    OPENAPI_CLIENT_SECRET="" \
    OPENAPI_GRANT_TYPE=password \
    OPENAPI_USERNAME="" \
    OPENAPI_PASSWORD="" \
    OPENAPI_BEARER_TOKEN="" \
    OPENAPI_API_KEY_NAME="" \
    OPENAPI_API_KEY_VALUE="" \
    OPENAPI_API_KEYS="" \
    OPENAPI_MAX_REQUESTS_PER_SECOND="1.7976931348623157e+308" \
    OPENAPI_MAX_SPLITS_PER_SECOND="1.7976931348623157e+308" \
    OPENAPI_DOMAIN_EXPANSION_LIMIT="256"

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["/usr/lib/trino/bin/run-trino"]
