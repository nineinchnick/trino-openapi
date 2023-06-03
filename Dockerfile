ARG TRINO_VERSION
FROM nineinchnick/trino-core:$TRINO_VERSION

ARG VERSION

ADD target/trino-openapi-$VERSION/ /usr/lib/trino/plugin/openapi/
ADD catalog/ /etc/trino/catalog/
ADD galaxy.spec.json /etc/trino/catalog/galaxy.spec.json
