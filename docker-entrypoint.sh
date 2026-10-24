#!/bin/bash

set -eo pipefail

catalog_dir=/etc/trino/catalog

if [ -n "$OPENAPI_SPEC_LOCATION" ]; then
    # skip properties of environment variables that are not set, to use their default values
    while IFS= read -r line; do
        if [[ $line =~ \$\{ENV:([A-Z_]+)\} && ! -v ${BASH_REMATCH[1]} ]]; then
            continue
        fi
        echo "$line"
    done <$catalog_dir/disabled/openapi.properties >$catalog_dir/openapi.properties
fi
if [ -n "$PETSTORE_URL" ]; then
    cp $catalog_dir/disabled/petstore.properties $catalog_dir/petstore.properties
fi
if [ -n "$JIRA_URL" ]; then
    cp $catalog_dir/disabled/jira.properties $catalog_dir/jira.properties
fi
if [ -n "$GITHUB_TOKEN" ]; then
    cp $catalog_dir/disabled/github.properties $catalog_dir/github.properties
fi
if [ -n "$GALAXY_URL" ]; then
    cp $catalog_dir/disabled/galaxy.properties $catalog_dir/galaxy.properties
fi
if [ -n "$DATADOG_URL" ]; then
    cp $catalog_dir/disabled/datadog.properties $catalog_dir/datadog.properties
fi
if [ -n "$CLOUDFLARE_API_TOKEN" ]; then
    cp $catalog_dir/disabled/cloudflare.properties $catalog_dir/cloudflare.properties
fi

exec "$@"
