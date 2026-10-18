Trino OpenAPI
=============

[![Build Status](https://github.com/nineinchnick/trino-openapi/actions/workflows/release.yaml/badge.svg)](https://github.com/nineinchnick/trino-openapi/actions/workflows/release.yaml)

A [Trino](https://trino.io/) connector that lets you query and modify data in
HTTP APIs with SQL, using only the API's OpenAPI specification.

Even with a specification, using an API means learning its domain first: which
endpoint returns which data, which parameters each one accepts, how results are
paged, and how errors are reported. APIs are designed with far fewer constraints
than SQL tables, so they're often inconsistent, even within a single service.

Filtering shows this best. Every endpoint accepts its own set of parameters,
often with names that don't match any field of the response, and usually only
for exact values. In SQL, any column of any table can be filtered, with the same
wide set of operators and functions.

With this connector, you point Trino at an OpenAPI specification, and every API
endpoint becomes a table. That lets you:

* discover what data is available with tools you already know, like
  `SHOW TABLES`, `DESCRIBE`, `information_schema`, or the schema browser of any
  SQL client;
* filter, sort, and aggregate on any column with standard SQL. Filters the API
  supports are sent in requests, and Trino applies the rest to the results;
* read all pages of a paginated endpoint as one result;
* join data from several APIs, or from an API and any other Trino catalog;
* change data with `INSERT`, `UPDATE`, and `DELETE`, instead of building
  requests.

All of this without writing a custom connector, or a script that copies the data
into a database.

For example, to count failed GitHub Actions runs of a repository by workflow
using `curl` and `jq`, you have to know how the endpoint pages its results, that
the runs are wrapped in a `workflow_runs` field, and then filter and group them
yourself:

```bash
page=1
while :; do
  runs=$(curl -s -H "Authorization: Bearer $GITHUB_TOKEN" \
    "https://api.github.com/repos/nineinchnick/trino-openapi/actions/runs?per_page=100&page=$page" \
    | jq '.workflow_runs')
  [ "$(jq length <<< "$runs")" -eq 0 ] && break
  echo "$runs"
  page=$((page + 1))
done | jq -s 'add
  | map(select(.conclusion == "failure"))
  | group_by(.name)
  | map({name: .[0].name, failures: length})
  | sort_by(-.failures)'
```

With the bundled GitHub catalog, it's a single query, which reads all pages:

```sql
SELECT name, count(*) AS failures
FROM github.default.repos_actions_runs
WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND conclusion = 'failure'
GROUP BY name
ORDER BY failures DESC;
```

The connector reads the specification when the catalog starts, maps paths to
tables and request parameters and response fields to columns, and translates
every query into HTTP requests.

# Quick start

Run a Trino server with the connector in a Docker container:

```bash
docker run \
  -d \
  --name trino-openapi \
  -e OPENAPI_SPEC_LOCATION=https://api.example.com/openapi.yaml \
  -e OPENAPI_BASE_URI=https://api.example.com/v1 \
  -p 8080:8080 \
  nineinchnick/trino-openapi:1.86
```

Then connect to Trino at http://localhost:8080 with any SQL client, and list
the tables of the `openapi` catalog:

```sql
SHOW TABLES FROM openapi.default;
```

The image also includes ready-made catalogs for some public APIs; see
[Bundled catalogs](#bundled-catalogs).

# Installation

Download the ZIP package of a [release](https://github.com/nineinchnick/trino-openapi/releases),
and extract it into the plugin directory on every node of your Trino cluster:

```bash
unzip trino-openapi-*.zip -d ${PLUGIN_DIRECTORY}/
mv ${PLUGIN_DIRECTORY}/trino-openapi-* ${PLUGIN_DIRECTORY}/openapi
```

Then create a catalog file, like `etc/catalog/openapi.properties`:

```properties
connector.name=openapi
spec-location=https://api.example.com/openapi.yaml
base-uri=https://api.example.com/v1
authentication.type=http
authentication.username=${ENV:OPENAPI_USERNAME}
authentication.password=${ENV:OPENAPI_PASSWORD}
```

Restart Trino. Every catalog can point to a different API.

# Configuration

| Property                       | Container environment variable    | Default   | Description                                                                                                 |
| ------------------------------ | --------------------------------- | --------- | ----------------------------------------------------------------------------------------------------------- |
| `spec-location`                | `OPENAPI_SPEC_LOCATION`           |           | URL or file path of the OpenAPI specification, in JSON or YAML                                              |
| `base-uri`                     | `OPENAPI_BASE_URI`                |           | Base URL of the API, often including the API version                                                        |
| `authentication.type`          | `OPENAPI_AUTH_TYPE`               | `none`    | Authentication used when the specification doesn't declare security: `none`, `http`, `api_key`, or `oauth`  |
| `authentication.scheme`        | `OPENAPI_AUTH_SCHEME`             | `basic`   | Scheme for the `http` type: `basic` or `bearer`                                                             |
| `authentication.username`      | `OPENAPI_USERNAME`                |           | Username for `http` basic authentication and the OAuth `password` flow                                      |
| `authentication.password`      | `OPENAPI_PASSWORD`                |           | Password for `http` basic authentication and the OAuth `password` flow                                      |
| `authentication.bearer-token`  | `OPENAPI_BEARER_TOKEN`            |           | Token for `http` bearer authentication                                                                      |
| `authentication.api-key-name`  | `OPENAPI_API_KEY_NAME`            |           | Name of the API key, when the specification doesn't declare it                                              |
| `authentication.api-key-value` | `OPENAPI_API_KEY_VALUE`           |           | Value of the API key                                                                                        |
| `authentication.api-keys`      | `OPENAPI_API_KEYS`                |           | Several API keys as `name=value` pairs separated by commas; can't be combined with the two properties above |
| `authentication.client-id`     | `OPENAPI_CLIENT_ID`               |           | OAuth client ID                                                                                             |
| `authentication.client-secret` | `OPENAPI_CLIENT_SECRET`           |           | OAuth client secret                                                                                         |
| `max-requests-per-second`      | `OPENAPI_MAX_REQUESTS_PER_SECOND` | unlimited | Maximum number of HTTP requests per second from a single Trino node                                         |
| `max-splits-per-second`        | `OPENAPI_MAX_SPLITS_PER_SECOND`   | unlimited | Maximum number of splits per second generated for a query                                                   |
| `domain-expansion-limit`       | `OPENAPI_DOMAIN_EXPANSION_LIMIT`  | `256`     | Maximum number of values a range predicate is expanded to, which limits the number of requests it can cause |

## Authentication

If the specification declares security schemes and requirements, the connector
uses them for every operation, with credentials from the catalog properties:

* `http` schemes use `authentication.username` and `authentication.password`
  for `basic`, or `authentication.bearer-token` for `bearer`;
* `apiKey` schemes send the key in a header, query parameter, or cookie, as the
  scheme declares, using `authentication.api-key-value`, or the matching key
  from `authentication.api-keys`;
* `oauth2` schemes request an access token from the `tokenUrl` of a flow that
  doesn't need a user to log in using a browser: the `password` flow, if the
  scheme declares it and `authentication.username` and `authentication.password`
  are set, otherwise the `clientCredentials` flow, using
  `authentication.client-id` and `authentication.client-secret`. Tokens are
  reused until shortly before they expire. The `authorizationCode` and `implicit`
  flows are not supported.

Operations with `security: []`, or specs with an empty top-level `security`
list, are called without credentials. If the specification doesn't declare any
security requirements, the connector uses `authentication.type`.

## HTTP client

The connector uses the Airlift HTTP client, which can be configured with
[its properties](https://github.com/airlift/airlift/blob/master/http-client/src/main/java/io/airlift/http/client/HttpClientConfig.java),
prefixed with `openApi`, for example:

```properties
openApi.http-client.request-timeout=30s
openApi.http-client.log.enabled=true
openApi.http-client.log.path=logs
```

# Usage

## Tables and columns

All tables are in the `default` schema. Use `SHOW TABLES` and `DESCRIBE` to see
how an API was mapped:

```sql
SHOW TABLES FROM openapi.default;
DESCRIBE openapi.default.pet;
```

A table has columns for:

* the fields of the `200` JSON response, or of the items selected by
  `resultsPath`;
* the request parameters (path, query, header, and cookie), including the ones
  shared by all operations of a path;
* the fields of JSON request bodies, like the ones of `POST`, `PUT`, and `PATCH`
  operations.

Request parameters are filters. Their values are copied from the query into
every returned row, because most APIs don't repeat them in responses.

## Queries

Filters on request parameter columns are sent in requests. If an operation
requires a parameter, the query must filter on its column:

```sql
SELECT name, status FROM petstore.default.pet WHERE pet_id = 1;
SELECT name FROM petstore.default.pet_find_by_status WHERE status = 'available';
```

Without the filter, the query fails with `Missing required constraint`.

* An equality filter sends a single value.
* An `IN` list makes one request per value, so it should stay short.
* A range, like `BETWEEN 1 AND 10`, is expanded into separate values when the
  column type allows it, up to `domain-expansion-limit`.
* Filters on other columns, and filters the selected request can't send, are
  applied by Trino to the returned rows.

Joins work too. When a filter value comes from another table, Trino passes it to
this connector as a dynamic filter, which is sent like any other filter:

```sql
SELECT p.name
FROM petstore.default.pet p
WHERE p.pet_id IN (SELECT pet_id FROM memory.default.favorite_pets);
```

## Changing data

`INSERT`, `UPDATE`, and `DELETE` send requests for every row, using the
operations described in [Schema mapping](#schema-mapping):

```sql
INSERT INTO petstore.default.pet (id, name, photo_urls, status)
VALUES (100, 'Cat X', ARRAY[], 'available');
UPDATE petstore.default.pet SET name = 'Cat Y' WHERE pet_id = 100;
DELETE FROM petstore.default.pet WHERE pet_id = 100;
```

An `UPDATE` using `PATCH` sends only the updated columns, and the required
fields of the request body, like identifiers. Setting a column to `NULL` sends
an explicit `null`. Other requests send all columns that are not `NULL`. A
`DELETE` sends the query filters, and when it uses an operation with a request
body, like a `POST` marked with `sqlOperation: delete`, the body includes the
filters on its fields.

A write succeeds if the server responds with any `2xx` status. Writes are not
transactional: if a statement fails halfway, the rows already sent stay
changed.

# Schema mapping

## Tables

Paths are mapped to tables. A table name is the path without path parameters,
with `camelCase` converted to `snake_case`, and other special characters
converted to underscores. Paths with path parameters are merged with the same
path without them, so `/pet` and `/pet/{petId}` become one table named `pet`.
If a path has several variants with different parameters, each variant becomes
a separate table that also includes the columns of the base path.

SQL statements use operations with these HTTP methods:

| SQL statement | HTTP method      |
| ------------- | ---------------- |
| `SELECT`      | `GET`            |
| `INSERT`      | `POST`           |
| `UPDATE`      | `PATCH` or `PUT` |
| `DELETE`      | `DELETE`         |

When a path has both `PATCH` and `PUT`, `UPDATE` uses `PATCH`, because an
`UPDATE` changes only some columns, and `PUT` replaces the whole resource.

Many APIs use `POST` to search, but the connector can't tell a search from an
operation that creates resources. A `POST` operation is used for `SELECT` only
when the specification marks it with `sqlOperation` in the
[`x-trino` extension](#openapi-extensions). This property can change the SQL
statements of any operation, and a marked operation is used only for the
statements it lists.

> [!IMPORTANT]
> Before version 1.88, `POST` operations were also used for `SELECT` when the
> path had no `GET`, and for `UPDATE` when the path had no `PUT`. Statements
> that used them now fail with `Table ... has no operation for ...`, until the
> specification sets `sqlOperation` in the `x-trino` extension of the
> operation, like `sqlOperation: select` for a search, or
> `sqlOperation: update` for a `POST` that changes a resource.

When several paths of a table can be used, the connector picks one for every
request: it skips paths whose required parameters are missing from the query,
and prefers the path using the most path parameters, then the most other
filters, then the shortest path.

## Columns

Column names follow the same rules as table names. Columns are disambiguated:

* if a request parameter has the same name as a response field but a different
  type, it gets a `_req` suffix;
* if different fields map to the same name, like `fooBar` and `foo_bar`, or a
  field has different types in different operations, the next ones get numeric
  suffixes (`_2`, `_3`, and so on).

Columns are nullable, unless the schema they come from lists them as required.
In tables that support `INSERT`, columns that only come from responses are
always nullable, because they can't be set in an `INSERT`.

Hidden columns don't appear in `SELECT *`, but can be selected and filtered:

* `__trino_row_id` identifies rows in `UPDATE` and `DELETE`;
* pagination parameters, like the page number and page size.

## Data types

| OpenAPI schema                                         | Trino type                                                             |
| ------------------------------------------------------ | ---------------------------------------------------------------------- |
| `integer` with `format: int32`                         | `INTEGER`                                                              |
| `integer` without a format, or `int64`                 | `BIGINT`                                                               |
| `number` with `format: float`                          | `REAL`                                                                 |
| `number` with `format: double`                         | `DOUBLE`                                                               |
| `number` without a format                              | `DECIMAL(18, 8)`                                                       |
| `boolean`                                              | `BOOLEAN`                                                              |
| `string`                                               | `VARCHAR`                                                              |
| `string` with `format: date`                           | `DATE`                                                                 |
| `string` with `format: date-time`                      | `TIMESTAMP(3)`, in UTC                                                 |
| `array`                                                | `ARRAY` of the item type                                               |
| `object` with `properties`                             | `ROW` with a field for every property                                  |
| `object` with `additionalProperties`                   | `MAP(VARCHAR, ...)` of the value type                                  |
| `type: [T, "null"]` (OpenAPI 3.1), or `nullable: true` | nullable `T`                                                           |
| unions, and anything else                              | see [Union types](#union-types), otherwise `VARCHAR` with the raw JSON |

## Union types

* When the response, or its array items, is a `oneOf`, `anyOf`, or `allOf`,
  the fields of all alternatives become columns. Fields of `oneOf` and `anyOf`
  alternatives are nullable, since a response matches only some of them, like a
  success or an error object.
* When some alternatives are arrays of objects, like a single file or a directory
  listing, every array element is a row, with its fields in the same columns.
* A field that is a union of only objects becomes a `ROW` with the fields of all
  alternatives, and a union of only arrays of objects becomes an `ARRAY` of such
  rows. Fields of other alternatives are `NULL` when reading, and are not sent in
  request bodies.
* A field defined in several alternatives with different types, and any other
  union, becomes a `VARCHAR` with the raw JSON value, which can be processed with
  [JSON functions](https://trino.io/docs/current/functions/json.html).

## Request serialization

Parameters are sent as the specification declares, including the `style`,
`explode`, and `allowReserved` settings, for arrays and objects. Path values are
percent-encoded. Parameters described with `content` instead of `schema` are
ignored. Request and response bodies must be JSON.

# OpenAPI extensions

Some behavior can't be described with standard OpenAPI. The connector reads an
`x-trino` extension in an operation:

```yaml
paths:
  /records:
    get:
      responses:
        # ...
      x-trino:
        pageParam: "page"
        limitParam: "per_page"
        resultsPath: "$response.body#/records"
        totalResultsPath: "$response.body#/total_count"
        errorPath: "$response.body#/message"
  /records/search:
    post:
      # ...
      x-trino:
        sqlOperation: select
  /records/{id}:
    put:
      # ...
      x-trino:
        sqlOperation: [insert, update]
```

| Property           | Description                                                                                                                                         |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pageParam`        | Parameter with the page number; enables pagination                                                                                                  |
| `limitParam`       | Parameter with the page size                                                                                                                        |
| `resultsPath`      | JSON pointer to the results in the response, like `$response.body#/items`                                                                           |
| `totalResultsPath` | JSON pointer to the total number of results                                                                                                         |
| `totalPagesPath`   | JSON pointer to the total number of pages                                                                                                           |
| `hasMorePath`      | JSON pointer to a boolean that is `false` on the last page                                                                                          |
| `maxPages`         | Maximum number of pages to request                                                                                                                  |
| `errorPath`        | JSON pointer to a human-readable message in error responses                                                                                         |
| `sqlOperation`     | SQL statements using the operation instead of the default ones: `select`, `insert`, `update`, `delete`, or several in a list or separated by commas |

If you can't change the API to include the extension in its specification, save
the specification locally and edit it.

## Pagination

The connector supports page number pagination: when `pageParam` is set and the
query doesn't filter on the page number, it requests page 1, 2, and so on, until
one of these happens:

* a page is empty;
* the number of results reaches the value at `totalResultsPath`;
* the number of pages reaches the value at `totalPagesPath`;
* the boolean value at `hasMorePath` is `false`;
* `maxPages` pages were requested;
* a page has fewer results than the page size, when `limitParam` is set and its
  value is known, either from a query filter or from the parameter's default in
  the spec. The page size is limited by the parameter's `maximum`, if declared,
  because servers often return fewer results than requested when the number is
  too high.

Pagination never guesses from page contents. If none of these apply, for
example when the server returns the last page for every page number past it and
declares no page size or total, the query makes requests until it's cancelled.
Set `maxPages` for such servers.

## Error messages

When the server responds with an error, the query fails with a message that
contains only the HTTP status code, not the response body, because the body
might not be readable, and it might contain sensitive information. Set
`errorPath` to include a readable message from the response.

# Bundled catalogs

The Docker image includes catalogs for some public APIs. Each catalog is enabled
when the first of its environment variables is set.

| API                                                                                               | Environment variables                        |
| ------------------------------------------------------------------------------------------------- | -------------------------------------------- |
| [OpenAPI Petstore](https://github.com/OpenAPITools/openapi-petstore)                              | `PETSTORE_URL`                               |
| [Starburst Galaxy](https://galaxy.starburst.io/public-api)                                        | `GALAXY_URL`, `GALAXY_TOKEN`                 |
| [Jira Cloud platform](https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/#version) | `JIRA_URL`, `JIRA_USERNAME`, `JIRA_PASSWORD` |
| [GitHub REST API](https://docs.github.com/en/rest)                                                | `GITHUB_TOKEN`                               |
| [Datadog API](https://docs.datadoghq.com/api/latest/)                                             | `DATADOG_URL`, `DD_API_KEY`, `DD_APP_KEY`    |

# Limitations

* The specification is read when the catalog starts. To use a changed
  specification, restart Trino.
* `base-uri` is required, even if the specification lists servers
  ([#51](https://github.com/nineinchnick/trino-openapi/issues/51)), and the
  specification is loaded without authentication
  ([#155](https://github.com/nineinchnick/trino-openapi/issues/155)).
* All tables are in a single `default` schema
  ([#16](https://github.com/nineinchnick/trino-openapi/issues/16)), with names
  derived from paths ([#15](https://github.com/nineinchnick/trino-openapi/issues/15),
  [#255](https://github.com/nineinchnick/trino-openapi/issues/255)).
* Without the `sqlOperation` extension, SQL statements are mapped from HTTP
  methods, so a `POST` search endpoint is used for `INSERT`, not for `SELECT`.
  The row ID used by `UPDATE` and `DELETE` comes from the query filters, not
  from the rows ([#254](https://github.com/nineinchnick/trino-openapi/issues/254)).
* Only equality and `IN` filters, and ranges expanded to single values, are sent
  in requests ([#5](https://github.com/nineinchnick/trino-openapi/issues/5)).
  `LIMIT` is not pushed down, so all pages are requested
  ([#93](https://github.com/nineinchnick/trino-openapi/issues/93)).
* Only page number pagination is supported, not offset or cursor pagination
  ([#96](https://github.com/nineinchnick/trino-openapi/issues/96)). Pages are
  requested one after another ([#94](https://github.com/nineinchnick/trino-openapi/issues/94)).
* Requests are not retried, so a temporary error fails the query
  ([#249](https://github.com/nineinchnick/trino-openapi/issues/249)), and
  responses are not cached ([#12](https://github.com/nineinchnick/trino-openapi/issues/12)).
* Only JSON request and response bodies are supported, not form-encoded or
  multipart bodies ([#250](https://github.com/nineinchnick/trino-openapi/issues/250)).
* Responses that are arrays of plain values
  ([#219](https://github.com/nineinchnick/trino-openapi/issues/219)), and objects
  with dynamic keys ([#172](https://github.com/nineinchnick/trino-openapi/issues/172)),
  are not mapped to rows. Some unions, like `anyOf` with a `null` alternative,
  are mapped to `VARCHAR` ([#52](https://github.com/nineinchnick/trino-openapi/issues/52)).
* Timestamps are always `TIMESTAMP(3)`.
* An OAuth token rejected by the server is not refreshed before it expires, and
  the OAuth flow can't be chosen explicitly
  ([#248](https://github.com/nineinchnick/trino-openapi/issues/248),
  [#10](https://github.com/nineinchnick/trino-openapi/issues/10)).
* Pagination metadata, like totals, is mapped to visible columns
  ([#137](https://github.com/nineinchnick/trino-openapi/issues/137)).

See all [open issues](https://github.com/nineinchnick/trino-openapi/issues) for
more.

# Design decisions

* **The specification is the only input.** Tables, columns, types, and requests
  are derived from it, and `x-trino` extensions describe what OpenAPI can't, so
  supporting a new API doesn't need code.
* **Path parameters identify resources.** That's why a path is merged with the
  same path without parameters into one table, and why its parameters are
  required filters.
* **Correct results before pushdown.** A filter is only removed from what Trino
  applies when every request that can be used sends it. A filter the selected
  request can't send is still applied by Trino, even if it means reading more
  data.
* **Explicit signals only.** Pagination stops only on signals the server or the
  spec provides, never by guessing from page contents. SQL statements are mapped
  from HTTP methods or the `sqlOperation` extension, never from operation names
  or summaries. OAuth flows that need a browser fail with a clear error instead
  of being attempted.
* **Columns are nullable unless the spec says otherwise,** because a field that
  isn't required can be missing from a response.
* **Unions keep all data.** Alternatives are merged when they're compatible, and
  kept as raw JSON when they're not, instead of choosing one of them.
* **Error responses are not shown to users,** unless `errorPath` selects a
  message, because they might contain sensitive information.
* **Exact values.** Decimals are read and sent without rounding through `double`,
  and dates and timestamps keep the precision of their column, in UTC.

# Development

Build the plugin and run all tests, which need Docker:

```bash
./mvnw clean install
```

The ZIP package is created in the `target` directory.

To try the connector locally, run `OpenApiQueryRunner` from the tests. Without
`OPENAPI_SPEC_LOCATION` and `OPENAPI_BASE_URI`, it starts a Petstore server in
Docker and creates an `openapi` catalog for it:

```bash
./mvnw test-compile exec:java -Dexec.mainClass="pl.net.was.OpenApiQueryRunner" -Dexec.classpathScope=test
```

Then connect with the Trino CLI using `trino --server localhost:8080`, and run
`SHOW TABLES FROM openapi.default`.
