#!/usr/bin/env bash

set -euo pipefail
set -x

OPTS=()
if [ -f release.properties ]; then
    VERSION=$(grep 'project.rel.pl.net.was\\:trino-openapi=' release.properties | cut -d'=' -f2)
    OPTS+=(--platform linux/amd64,linux/arm64 --push)
else
    VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
    OPTS+=(--load)
fi
TRINO_VERSION=$(mvn help:evaluate -Dexpression=dep.trino.version -q -DforceStdout)
TAG=nineinchnick/trino-openapi:$VERSION

docker buildx build \
    -t "$TAG" \
    --build-arg TRINO_VERSION="$TRINO_VERSION" \
    --build-arg VERSION="$VERSION" \
    "${OPTS[@]}" \
    .
