Trino Plugin
============

[![Build Status](https://github.com/nineinchnick/trino-openapi/actions/workflows/release.yaml/badge.svg)](https://github.com/nineinchnick/trino-openapi/actions/workflows/release.yaml)

This is a [Trino](http://trino.io/) plugin that provides a connector to read from HTTP APIs given an OpenAPI specification.

# Quick Start

To run a Docker container with the connector, run the following:
```bash
docker run \
  -d \
  --name trino-openapi \
  -e OPENAPI_SPEC_LOCATION=galaxy.spec.json \
  -e OPENAPI_BASE_URI=https://ping.galaxy-dev.io \
  -e OPENAPI_CLIENT_ID \
  -e OPENAPI_CLIENT_SECRET \
  -p 8080:8080 \
  nineinchnick/trino-openapi:0.1
```

Then use your favourite SQL client to connect to Trino running at http://localhost:8080

## Usage

Download one of the ZIP packages, unzip it and copy the `trino-openapi-0.1` directory to the plugin directory on every node in your Trino cluster.
Create a `openapi.properties` file in your Trino catalog directory and set all the required properties.

```
connector.name=openapi
spec-location=https://galaxy.starburst.io/public/openapi/v1/json
base-uri=https://ping.galaxy.starburst.io
authentication.type=client-credentials
authentication.client-id=${ENV:OPENAPI_CLIENT_ID}
authentication.client-secret=${ENV:OPENAPI_CLIENT_SECRET}
```

After reloading Trino, you should be able to connect to the `openapi` catalog.

## Build

Run all the unit tests:
```bash
mvn test
```

Creates a deployable zip file:
```bash
mvn clean package
```

Unzip the archive from the target directory to use the connector in your Trino cluster.
```bash
unzip target/*.zip -d ${PLUGIN_DIRECTORY}/
mv ${PLUGIN_DIRECTORY}/trino-openapi-* ${PLUGIN_DIRECTORY}/trino-openapi
```

## Debug

To test and debug the connector locally, run the `OpenApiQueryRunner` class located in tests:
```bash
mvn test-compile exec:java -Dexec.mainClass="pl.net.was.OpenApiQueryRunner" -Dexec.classpathScope=test
```

And then run the Trino CLI using `trino --server localhost:8080 --no-progress` and query it:
```
trino> show catalogs;
 Catalog
---------
 openapi
 system
(2 rows)

trino> show tables from openapi.default;
   Table
------------
 single_row
(1 row)

trino> select * from openapi.default.single_row;
 id |     type      |  name
----+---------------+---------
 x  | default-value | my-name
(1 row)
```


## API Token configuration

# Providers example
- Starburst Galaxy
  - https://docs.starburst.io/starburst-galaxy/access-control/api-auth-token.html
- Atlassian Jira
  - https://developer.atlassian.com/cloud/jira/platform/basic-auth-for-rest-apis/#get-an-api-token
