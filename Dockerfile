ARG TRINO_VERSION
FROM nineinchnick/trino-core:$TRINO_VERSION

ARG VERSION

ADD target/trino-openapi-$VERSION/ /usr/lib/trino/plugin/openapi/
ADD catalog/ /etc/trino/catalog/
ADD petstore.yaml /etc/trino/catalog/petstore.yaml
