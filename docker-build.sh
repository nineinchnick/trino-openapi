#!/usr/bin/env bash

set -euo pipefail
set -x

MVN=./mvnw
MOPTS=(-q -DforceStdout)
if command -v mvnd >/dev/null; then
    MVN=mvnd
    MOPTS+=(--raw-streams)
fi
OPTS=()
if [ -f release.properties ]; then
    VERSION=$(grep 'project.rel.pl.net.was\\:trino-openapi=' release.properties | cut -d'=' -f2)
    OPTS+=(--platform "linux/amd64,linux/arm64" --push)
else
    VERSION=$("$MVN" help:evaluate -Dexpression=project.version "${MOPTS[@]}")
    OPTS+=(--load)
fi
TRINO_VERSION=$("$MVN" help:evaluate -Dexpression=dep.trino.version "${MOPTS[@]}")
TAG=nineinchnick/trino-openapi:$VERSION

docker buildx build \
    -t "$TAG" \
    --build-arg TRINO_VERSION="$TRINO_VERSION" \
    --build-arg VERSION="$VERSION" \
    "${OPTS[@]}" \
    .
