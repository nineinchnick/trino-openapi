ARG TRINO_VERSION
FROM nineinchnick/trino-core:$TRINO_VERSION

ARG VERSION

ADD target/trino-openapi-$VERSION/ /usr/lib/trino/plugin/openapi/
ADD catalog/openapi.properties /etc/trino/catalog/openapi.properties
