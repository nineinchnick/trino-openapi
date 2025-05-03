Trino OpenAPI
=============

[![Build Status](https://github.com/nineinchnick/trino-openapi/actions/workflows/release.yaml/badge.svg)](https://github.com/nineinchnick/trino-openapi/actions/workflows/release.yaml)

This is a [Trino](http://trino.io/) plugin that provides a connector to read
from and write to HTTP APIs given an OpenAPI specification.

# Quick Start

To run a Docker container with the connector, run the following:
```bash
docker run \
  -d \
  --name trino-openapi \
  -e OPENAPI_SPEC_LOCATION=http://api.example/api/spec.yaml \
  -e OPENAPI_BASE_URI=http://api.example/api/v1 \
  -p 8080:8080 \
  nineinchnick/trino-openapi:1.66
```

Then use your favourite SQL client to connect to Trino running at
http://localhost:8080

## Usage

Download one of the ZIP packages, unzip it and copy the `trino-openapi-1.66`
directory to the plugin directory on every node in your Trino cluster. Create a
`openapi.properties` file in your Trino catalog directory and set all the
required properties.

```
connector.name=openapi
spec-location=http://api.example/api/spec.yaml
base-uri=http://api.example/api/v1
authentication.type=http
authentication.username=${ENV:OPENAPI_USERNAME}
authentication.password=${ENV:OPENAPI_PASSWORD}
```

After reloading Trino, you should be able to connect to the `openapi` catalog.

## Configuration

| Configuration property        | Container environmental variable | Description                                                                                              |
|-------------------------------|----------------------------------|----------------------------------------------------------------------------------------------------------|
| spec-location                 | `OPENAPI_SPEC_LOCATION`          | URL or filename containing the OpenAPI specification, either JSON or YAML                                |
| base-uri                      | `OPENAPI_BASE_URI`               | Base URL for the API, often includes API version                                                         |
| authentication.type           | `OPENAPI_AUTH_TYPE`              | Default authentication type if not set in the specification. One of: `none`, `http`, `api_key`, `oauth`. |
| authentication.scheme         | `OPENAPI_AUTH_SCHEME`            | Authentication scheme for the `http` authentication type. One of: `basic`, `bearer`.                     |
| authentication.client-id      | `OPENAPI_CLIENT_ID`              | OAuth Client ID                                                                                          |
| authentication.client-secret  | `OPENAPI_CLIENT_SECRET`          | OAuth Client secret                                                                                      |
| authentication.username       | `OPENAPI_USERNAME`               | Username used for the `http` and `oauth` authentication types                                            |
| authentication.password       | `OPENAPI_PASSWORD`               | Password used for the `http` and `oauth` authentication types                                            |
| authentication.bearer-token   | `OPENAPI_BEARER_TOKEN`           | Bearer token for `http` authentication                                                                   |
| authentication.api-key-name   | `OPENAPI_API_KEY_NAME`           | API key name                                                                                             |
| authentication.api-key-value  | `OPENAPI_API_KEY_VALUE`          | API key value                                                                                            |
| max-requests-per-second       | `OPENAPI_MAX_REQUESTS_PER_SECOND`| Maximum HTTP requests per second executed from a single Trino node                                       |
| max-splits-per-second         | `OPENAPI_MAX_SPLITS_PER_SECOND`  | Maximum number of splits per second generated when executing a query                                     |
| domain-expansion-limit        | `OPENAPI_DOMAIN_EXPANSION_LIMIT` | Maximum number of discrete domain values generated from range predicates, which correlates with the number of HTTP requests executed |

The connector is using the Airlift HTTP client, which can be configured with
[additional
properties](https://github.com/airlift/airlift/blob/master/http-client/src/main/java/io/airlift/http/client/HttpClientConfig.java)
prefixed with `openApi`, like so:

```
openApi.http-client.log.enabled=true
openApi.http-client.log.path=logs
```

## Known APIs

This connector has been tested with the following APIs. The Docker image
includes additional catalog files that will be used if the first of their
environmental variable is set.

| Name                                                                                              | Environmental variables      |
|---------------------------------------------------------------------------------------------------|------------------------------|
| [OpenAPI Petstore](https://github.com/OpenAPITools/openapi-petstore)                              | `PETSTORE_URL`               |
| [Starburst Galaxy](https://galaxy.starburst.io/public-api)                                        | `GALAXY_URL`, `GALAXY_TOKEN` |
| [Jira Cloud platform](https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/#version) | `JIRA_URL`                   |
| [GitHub REST API](https://docs.github.com/en/rest)                                                | `GITHUB_TOKEN`               |
| [DataDog API](https://docs.datadoghq.com/api/latest/)                                             | `DATADOG_URL`                |

## Schema mapping

The following rules are used to map the OpenAPI schema to SQL tables and columns:
* Paths are mapped to tables; table names are paths, with `camelCase` converted
  to `snake_case` and special characters (like `/`) converted to underscores.
* SQL operations are mapped to HTTP methods:
  * SELECT uses GET or POST, if no GET is available;
  * INSERT uses POST or PUT;
  * UPDATE uses PATCH or POST;
  * DELETE uses DELETE.
* All request parameters are mapped to columns, including path, query, and
  header parameters.
* Fields of the HTTP OK (200) response type are mapped to columns.
* Fields of the request body are also mapped to columns, if a POST request is
  used for SELECT.
* All columns are disambiguated - if a field with the same name but a different
  data type appears in the response and parameters, it'll be mapped to multiple
  columns, with numeric suffixes (`_2`, `_3`, etc.). Request body fields have a
  `_req` suffix.

## OpenAPI Extensions

OpenAPI allows using custom extensions - adding custom fields anywhere in the
schema as long as they're prefixed with an `X`. Such custom extensions are used
to fine-tune this connector.

If the original service cannot be modified to include an extension in a
generated OpenAPI schema, save it locally and modify as needed.

### Pagination

APIs can use 4 different types of pagination:
* Offset - every response can include an offset parameter, telling how many
  results to skip.
* Page - every response can include a page parameter; the number of results per
  page can be configurable with another parameter.
* Cursor/token - the response includes the value of a cursor or token,
  that needs to be included in the next request.
* Next page URL - the response includes an URL of the next set of results.

The connector currently supports only the `Page` pagination.

To enable pagination, add a `x-pagination` section in the path's operation section:

```
paths:
  /records:
    get:
      responses:
        # ...
      x-pagination:
       pageParam: "page"
       limitParam: "per-page"
       resultsPath: "$response.body#/workflows"
       totalResultsPath: "$response.body#/total_count"
```

## Build

Run all the unit tests:
```bash
mvn test
```

Creates a deployable zip file:
```bash
mvn clean package
```

Unzip the archive from the target directory to use the connector in your Trino
cluster.
```bash
unzip target/*.zip -d ${PLUGIN_DIRECTORY}/
mv ${PLUGIN_DIRECTORY}/trino-openapi-* ${PLUGIN_DIRECTORY}/trino-openapi
```

## Debug

To test and debug the connector locally, run the `OpenApiQueryRunner` class
located in tests:
```bash
mvn test-compile exec:java -Dexec.mainClass="pl.net.was.OpenApiQueryRunner" -Dexec.classpathScope=test
```

And then run the Trino CLI using `trino --server localhost:8080 --no-progress`
and query it:
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
