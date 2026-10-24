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

Run a Trino server with the latest release of the connector in a Docker
container:

```bash
VERSION=$(curl -fsSL https://api.github.com/repos/nineinchnick/trino-openapi/releases/latest | sed -n 's/.*"tag_name": *"v\([^"]*\)".*/\1/p')
docker run \
  -d \
  --name trino-openapi \
  -e OPENAPI_SPEC_LOCATION=https://api.example.com/openapi.yaml \
  -e OPENAPI_BASE_URI=https://api.example.com/v1 \
  -p 8080:8080 \
  nineinchnick/trino-openapi:$VERSION
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

Use a release built for the version of your Trino server, because the plugin
might not work with other Trino versions. The notes of every
[release](https://github.com/nineinchnick/trino-openapi/releases) list the
Trino version it's built for.

# Configuration

| Property                             | Container environment variable      | Default   | Description                                                                                                     |
| ------------------------------------ | ----------------------------------- | --------- | --------------------------------------------------------------------------------------------------------------- |
| `spec-location`                      | `OPENAPI_SPEC_LOCATION`             |           | URL or file path of the OpenAPI specification, in JSON or YAML                                                  |
| `base-uri`                           | `OPENAPI_BASE_URI`                  |           | Base URL of the API, often including the API version                                                            |
| `authentication.type`                | `OPENAPI_AUTH_TYPE`                 | `none`    | Authentication used when the specification doesn't declare security: `none`, `http`, or `api_key`              |
| `authentication.scheme`              | `OPENAPI_AUTH_SCHEME`               | `basic`   | Scheme for the `http` type: `basic` or `bearer`                                                                 |
| `authentication.username`            | `OPENAPI_USERNAME`                  |           | Username for `http` basic authentication and the OAuth `password` flow                                          |
| `authentication.password`            | `OPENAPI_PASSWORD`                  |           | Password for `http` basic authentication and the OAuth `password` flow                                          |
| `authentication.bearer-token`        | `OPENAPI_BEARER_TOKEN`              |           | Token for `http` bearer authentication                                                                          |
| `authentication.api-key-name`        | `OPENAPI_API_KEY_NAME`              |           | Name of the API key, when the specification doesn't declare it                                                  |
| `authentication.api-key-value`       | `OPENAPI_API_KEY_VALUE`             |           | Value of the API key                                                                                            |
| `authentication.api-keys`            | `OPENAPI_API_KEYS`                  |           | Several API keys as `name=value` pairs separated by commas; can't be combined with the two properties above     |
| `authentication.client-id`           | `OPENAPI_CLIENT_ID`                 |           | OAuth client ID                                                                                                 |
| `authentication.client-secret`       | `OPENAPI_CLIENT_SECRET`             |           | OAuth client secret                                                                                             |
| `authentication.token-url`           | `OPENAPI_TOKEN_URL`                 |           | OAuth token endpoint, used instead of the `tokenUrl` of the specification                                       |
| `authentication.allow-insecure-http` | `OPENAPI_ALLOW_INSECURE_HTTP`       | `false`   | Allow sending credentials over HTTP, without TLS                                                                |
| `max-requests-per-second`            | `OPENAPI_MAX_REQUESTS_PER_SECOND`   | unlimited | Maximum number of HTTP requests per second from a single Trino node, greater than `0`; unlimited if not set     |
| `max-splits-per-second`              | `OPENAPI_MAX_SPLITS_PER_SECOND`     | unlimited | Maximum number of splits per second generated for a query, greater than `0`; unlimited if not set               |
| `max-retries`                        | `OPENAPI_MAX_RETRIES`               | `3`       | Maximum number of times a request that failed with a transient error is retried; `0` disables retries           |
| `retry-initial-delay`                | `OPENAPI_RETRY_INITIAL_DELAY`       | `1s`      | Delay before the first retry, doubled for every next retry                                                      |
| `retry-max-delay`                    | `OPENAPI_RETRY_MAX_DELAY`           | `1m`      | Maximum delay between retries; fails if the server asks to wait longer with `Retry-After`                       |
| `domain-expansion-limit`             | `OPENAPI_DOMAIN_EXPANSION_LIMIT`    | `256`     | Maximum number of values a range predicate is expanded to, which limits the number of requests it can cause     |
| `http-cache.directory`               | `OPENAPI_HTTP_CACHE_DIRECTORY`      |           | Directory to cache HTTP responses in; responses are not cached when it's not set. See [HTTP cache](#http-cache) |
| `http-cache.max-size`                | `OPENAPI_HTTP_CACHE_MAX_SIZE`       | `1GB`     | Maximum size of the cache of a catalog on a node; the least recently used responses are removed above it        |
| `http-cache.max-entry-size`          | `OPENAPI_HTTP_CACHE_MAX_ENTRY_SIZE` | `10MB`    | Maximum size of a single cached response; larger responses are not cached                                       |
| `split-strategy`                     | `OPENAPI_SPLIT_STRATEGY`            | `off`     | How to read pages in separate splits: `probe`, `stride`, or `off`; see [Pagination](#pagination)                |
| `probe-limit`                        | `OPENAPI_PROBE_LIMIT`               | `none`    | Page size of the first page requested by the `probe` split strategy, or `none` to use the regular page size     |

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

When an operation lists several security requirements, the connector uses the
first one whose credentials are all configured, and fails the query, naming the
missing properties, if there's none.

Operations with `security: []`, or specs with an empty top-level `security`
list, are called without credentials. If the specification doesn't declare any
security requirements, the connector uses `authentication.type`. It can't be
`oauth`, because OAuth needs the token URL and flows of an `oauth2` security
scheme declared in the specification, so the catalog fails to start with it.

The specification decides where and how credentials are sent: which operations
get them, whether an API key goes in a header or the query string, and where
OAuth credentials are sent to get a token. Only use specifications from a source
you trust as much as the API itself. To limit what a specification can change:

* requests are only sent to `base-uri`, and redirects are not followed, because
  the HTTP client would send custom headers, like API keys, to the new location;
* a `tokenUrl` from the specification must use HTTPS, or the host of
  `base-uri`; set `authentication.token-url` to use another one;
* credentials are only sent over HTTPS, unless
  `authentication.allow-insecure-http` is set, for example for an API on a local
  network.

## HTTP client

The connector uses the Airlift HTTP client, which can be configured with
[its properties](https://github.com/airlift/airlift/blob/master/http-client/src/main/java/io/airlift/http/client/HttpClientConfig.java),
prefixed with `openApi`, for example:

```properties
openApi.http-client.request-timeout=30s
openApi.http-client.log.enabled=true
openApi.http-client.log.path=logs
```

The request log, and request spans when Trino has
[tracing](https://trino.io/docs/current/admin/opentelemetry.html) enabled,
include the full URL of every request. When an API key is sent in the query
string, it's recorded there too, so prefer sending keys in a header, if the API
supports it, or keep the request log and traces as protected as the
credentials.

The connector asks servers for gzip-compressed responses, with
`Accept-Encoding: gzip`, and decompresses them, because JSON usually compresses
about ten times.

## Retries

Requests that fail with a transient error are retried up to `max-retries`
times. A transient error is a network error, like a timeout or a closed
connection, or a response with one of these HTTP status codes:

* `429 Too Many Requests`, `502 Bad Gateway`, `503 Service Unavailable`, and
  `504 Gateway Timeout`;
* `403 Forbidden` with a `Retry-After` header, or with headers saying that the
  rate limit is exhausted, as declared in the spec, which some APIs, like
  GitHub, use for exceeded rate limits.

Requests that have the same effect when they're repeated are retried after any
transient error: `GET`, `HEAD`, `OPTIONS`, `PUT`, `DELETE`, and `POST`
operations used only for `SELECT`, like searches. Other requests using `POST`,
like `INSERT` statements, and `PATCH`, like most `UPDATE` statements, are
retried only after `429 Too Many Requests`, which means that the server didn't
process them. After other errors, repeating them can create duplicates or apply
a change twice.

When a retried `DELETE` gets `404 Not Found` or `410 Gone`, it succeeds,
because the previous attempt could have deleted the resource before failing,
for example with `504 Gateway Timeout`. The first attempt still fails on these
errors.

The delay before a retry starts at `retry-initial-delay` and doubles after
every retry, up to `retry-max-delay`. The connector waits a random time between
half of that delay and the whole delay, so many queries or splits that failed at
the same time don't retry at the same time. Every retry is logged at the `INFO`
level.

If the response has a `Retry-After` header, the connector waits exactly as long
as it asks for instead. If that's longer than `retry-max-delay`, the query fails
right away, because retrying earlier than the server allows can get the client
blocked, for example by GitHub. Increase `retry-max-delay` to wait longer.

Many APIs, like [GitHub](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api),
respond to an exceeded rate limit without `Retry-After`, but with headers
saying when it resets. Declare them in the `x-trino` extension of the whole
document, so the connector waits until then, instead of failing the query:

```yaml
openapi: 3.0.3
x-trino:
  rateLimitResetHeader: "x-ratelimit-reset"
  rateLimitResetFormat: "epochSeconds"
  rateLimitRemainingHeader: "x-ratelimit-remaining"
paths:
  # ...
```

| Property                   | Description                                                                                                                                              |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `rateLimitResetHeader`     | Response header with the time the rate limit resets                                                                                                      |
| `rateLimitResetFormat`     | Format of the reset time: `epochSeconds` for seconds since the Unix epoch, in UTC, or `delaySeconds` for seconds to wait; required with the reset header |
| `rateLimitRemainingHeader` | Response header with the number of requests left; the reset time is used only when it's `0`                                                              |

When a `403 Forbidden` or `429 Too Many Requests` response has the reset header,
and the remaining header is `0`, if declared, the connector waits until the
reset time, plus a random time up to a second, so requests waiting for the same
reset aren't all sent at once. Like with `Retry-After`, the query fails right
away if that's longer than `retry-max-delay`, and every wait counts against
`max-retries`. `Retry-After` takes precedence over the reset header.

Declare `rateLimitRemainingHeader` if the API sends the reset header in every
response, like GitHub, because otherwise a `403 Forbidden` caused by missing
permissions would be retried too.

Retries count against `max-requests-per-second`, like any other request.
Error responses, including exceeded rate limits, are never stored in the
[HTTP cache](#http-cache), and they don't remove a cached response, so a retry
can revalidate it.

Waiting before a retry, up to `retry-max-delay`, which is 1 minute by default,
and waiting for `max-requests-per-second`, blocks the thread that sends the
request, which is usually a Trino driver thread. Driver threads are shared by
all queries running on a node, so when many splits wait at the same time, other
queries, also ones not using this connector, can run slower, or wait for a free
thread.

## HTTP cache

To avoid sending the same requests again, set `http-cache.directory`, and the
connector caches responses on disk, following
[RFC 9111](https://www.rfc-editor.org/rfc/rfc9111). Cached responses survive
restarts. Every catalog uses its own subdirectory, named after the catalog and
a hash of `base-uri` and the account, so a catalog created again with the same
name, but for another server or account, doesn't use responses cached for the
previous one. The account is identified by the username, client ID, and API key
names, so changing a password or a client secret keeps the cached responses.
Bearer tokens and API key values are the only identity of their accounts, so
they're included with a slow hash, PBKDF2, which makes guessing them from the
directory name impractical. Subdirectories of removed or changed catalogs are not
deleted. The connector creates the subdirectories and files so that only the
user running Trino can read them, and it doesn't store the `Set-Cookie` and
`Authentication-Info` response headers.

* Responses to `GET` and `HEAD` requests are cached when the server says how
  long they're fresh, with `Cache-Control: max-age` or `Expires`, or how to check
  if they changed, with `ETag` or `Last-Modified`. Responses with
  `Cache-Control: no-store` are never cached.
* Responses are cached separately for every value of header and cookie
  parameters, because they filter the response, and servers rarely list them in
  the `Vary` header. Credentials are not part of the key, because every catalog
  uses its own subdirectory.
* A fresh response is reused without sending a request.
* A response that is no longer fresh, or has no freshness information, is
  revalidated: the request includes `If-None-Match` or `If-Modified-Since`, and
  if the server responds with `304 Not Modified`, the cached response is used.
  Some APIs, like GitHub, don't count such requests against rate limits.
* A successful `INSERT`, `UPDATE`, or `DELETE` removes cached responses of the
  same URL, for all values of header and cookie parameters, on the node that
  ran it.
* A response that can't be stored removes the cached one, if it's successful,
  or says that the resource doesn't exist, with `404 Not Found` or `410 Gone`.
  Other errors, like exceeded rate limits or server failures, say nothing about
  the resource, so the cached response is kept, and can be revalidated later.
* When the cache of a node is bigger than `http-cache.max-size`, the least
  recently used responses are removed. Responses bigger than `http-cache.max-entry-size` are
  not cached.
* Compressed responses are cached as they were received, so they take less
  space, and the size limits apply to their compressed size.

Every node has its own cache, and its own size limit, and a change on one node
doesn't remove responses cached by other nodes, which can return them until
they're no longer fresh. Use a directory on a local disk of every node, not a
volume shared by nodes, or by several Trino processes. Such processes can read
and write responses at the same time, but every process only tracks responses
it stored or found when it started, so the total size is not limited, and a
change doesn't remove responses stored by other processes.

The connector never guesses how long a response is fresh, so it never returns
data older than the server allows. To make a query check every cached response
with the server, set the `http_cache_enabled` session property of the catalog to
`false`:

```sql
SET SESSION openapi.http_cache_enabled = false;
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
every returned row, because most APIs don't repeat them in responses. If the
query doesn't filter on a parameter that has a value in
[`parameterDefaults`](#parameter-defaults), rows have that value. A path
parameter [linked to a response field](#resource-identifiers) is the column of
that field instead, and rows have the value from the response.

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
* A range on a response column, like `updated_at >= TIMESTAMP '2024-01-01'`, is
  sent as lower and upper bound parameters, when the operation maps them to
  the column with [range filters](#range-filters).
* Filters on other columns are applied by Trino to the returned rows.
* When a table is read using several requests, like listing items or getting
  a single one, a filter that the selected request can't send is applied by the
  connector to the returned rows, so it doesn't prevent pushing down `LIMIT`.

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
filters on its fields. When the request body is an array, like in
`POST /user/createWithList` of the Petstore API, its columns are the fields of
the array items, and every row is sent as an array with a single item.

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
a separate table that also includes the columns of the base path, and the table
name also includes the path parameters.

Different paths can map to the same table name, like `/a/b` and `/a-b`. They
become separate tables, and the next ones in the order of their paths get
numeric suffixes (`_2`, `_3`, and so on), skipping names of other tables. The
connector logs a warning with such paths.

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
> Before version 2.0, `POST` operations were also used for `SELECT` when the
> path had no `GET`, and for `UPDATE` when the path had no `PUT`. Statements
> that used them now fail with `Table ... has no operation for ...`, until the
> specification sets `sqlOperation` in the `x-trino` extension of the
> operation, like `sqlOperation: select` for a search, or
> `sqlOperation: update` for a `POST` that changes a resource. A marked
> operation is used only for the statements it lists, so marking a search with
> `sqlOperation: select` also stops `INSERT` from using it.

When several paths of a table can be used, the connector picks one for every
request: it skips paths whose required parameters are missing from the query,
and prefers the path using the most path parameters, then the most other
filters, then the shortest path.

`EXPLAIN` shows the paths of the table, and the predicates, `LIMIT`, and
`ORDER BY` pushed down into requests, in the `TableScan` node:

```text
TableScan[table = petstore:store_order paths=[GET /store/order/{orderId}] constraint={order_id=[ SortedRangeSet[type=bigint, ranges=1, {[1]}] ]}]
```

## Columns

Column names follow the same rules as table names. Columns are disambiguated:

* if a request parameter has the same name as a response field but a different
  type, it gets a `_req` suffix;
* if different fields map to the same name, like `fooBar` and `foo_bar`, or a
  field has different types in different operations, the next ones get numeric
  suffixes (`_2`, `_3`, and so on).

Columns are nullable, unless the schema they come from lists them as required.
Columns built from responses are nullable, unless they're required and not
nullable in responses of all operations used for `SELECT`, because a query uses
only one of them, depending on its filters.
In tables that support `INSERT`, columns that only come from responses are
always nullable, because they can't be set in an `INSERT`.

Hidden columns don't appear in `SELECT *`, but can be selected and filtered:

* `__trino_row_id` identifies rows in `UPDATE` and `DELETE`;
* pagination parameters, like the page number and page size;
* response fields next to the results at `resultsPath`, like the total number
  of results, unless another operation returns them as a field of its results;
  every row has the value from the response of its page.

## Data types

| OpenAPI schema                                         | Trino type                                                             |
| ------------------------------------------------------ | ---------------------------------------------------------------------- |
| `integer` with `format: int32`                         | `INTEGER`                                                              |
| `integer` without a format, or `int64` or `uint32`     | `BIGINT`                                                               |
| `integer` with `format: uint64`                        | `DECIMAL(20, 0)`                                                       |
| `number` with `format: float`                          | `REAL`                                                                 |
| `number` with `format: double`                         | `DOUBLE`                                                               |
| `number` without a format                              | `DECIMAL(38, 10)`                                                      |
| `boolean`                                              | `BOOLEAN`                                                              |
| `string`                                               | `VARCHAR`                                                              |
| `string` with `format: date`                           | `DATE`                                                                 |
| `string` with `format: date-time`                      | `TIMESTAMP(3)`, in UTC                                                 |
| `array`                                                | `ARRAY` of the item type                                               |
| `object` with `properties`                             | `ROW` with a field for every property                                  |
| `object` with `additionalProperties`                   | `MAP(VARCHAR, ...)` of the value type                                  |
| `type: [T, "null"]` (OpenAPI 3.1), or `nullable: true` | nullable `T`                                                           |
| `oneOf` or `anyOf` of `T` and `type: "null"`           | nullable `T`                                                           |
| unions, and anything else                              | see [Union types](#union-types), otherwise `VARCHAR` with the raw JSON |

Integers outside the range of the column type, or with a fractional part, and
numbers with more than 28 integer digits fail the query, because the API sent a
value that doesn't match its specification. Numbers with more than 10 fractional
digits are rounded half up.

JSON values of a different type than the column, like an array in a `MAP` or
`ROW` column, fail the query too. Numbers and booleans can be sent as JSON
strings, like `"12.50"` or `"true"`, and booleans as `0` and `1`.

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

`VARCHAR` columns of unions, and of schemas that can't be mapped to a Trino
type, hold JSON text. Strings are quoted, so for `oneOf: [string, integer]`, the
string `"ci"` is read as `'"ci"'`, and the number `7` as `'7'`. Values written to
such columns in request bodies must be valid JSON, and are sent as the parsed
value, so `'7'` is sent as a number, and `'"7"'` as a string. Other values, like
`'ci'`, fail the query.

> [!IMPORTANT]
> Before version 2.2, strings in these columns were read without quotes, and
> values were sent in request bodies without checking that they're valid JSON.
> Queries that compare such columns with plain strings, like `value = 'ci'`,
> must use quoted strings, like `value = '"ci"'`, or JSON functions, like
> `json_extract_scalar(value, '$') = 'ci'`.

## Request serialization

Parameters are sent as the specification declares, including the `style`,
`explode`, and `allowReserved` settings, for arrays and objects. Path values are
percent-encoded. Parameters described with `content` instead of `schema` are
ignored. Request and response bodies must be JSON.

# OpenAPI extensions

Some behavior can't be described with standard OpenAPI. The connector reads an
`x-trino` extension in an operation, one in a path parameter, which links it to
a [response field](#resource-identifiers), and one of the whole document, which
describes [rate limits](#retries) and [error messages](#error-messages):

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
        sortParam: "sort"
        sortValues:
          updated_at: "updated"
        directionParam: "direction"
        ascendingValue: "asc"
        descendingValue: "desc"
        parameterDefaults:
          per_page: 100
          state: "all"
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

| Property            | Description                                                                                                                                         |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pageParam`         | Parameter with the page number; enables pagination                                                                                                  |
| `limitParam`        | Parameter with the page size                                                                                                                        |
| `resultsPath`       | JSON pointer to the results in the response, like `$response.body#/items`                                                                           |
| `totalResultsPath`  | JSON pointer to the total number of results, a number or a string with a number                                                                     |
| `totalPagesPath`    | JSON pointer to the total number of pages, a number or a string with a number                                                                       |
| `hasMorePath`       | JSON pointer to a boolean that is `false` on the last page                                                                                          |
| `maxPages`          | Maximum number of pages to request                                                                                                                  |
| `maxResults`        | Maximum number of results to read; pages past it are not requested                                                                                  |
| `splitStrategy`     | How to read pages in separate splits, overriding the `split-strategy` catalog property                                                              |
| `strideSplits`      | Number of splits reading pages with the `stride` strategy, instead of the number of worker nodes                                                    |
| `probeLimit`        | Page size of the first page requested by the `probe` split strategy, overriding the `probe-limit` catalog property                                  |
| `errorPath`         | JSON pointer to a human-readable message in error responses                                                                                         |
| `rangeFilters`      | Parameters that limit values of response fields, see below                                                                                          |
| `sqlOperation`      | SQL statements using the operation instead of the default ones: `select`, `insert`, `update`, `delete`, or several in a list or separated by commas |
| `sortParam`         | Parameter that selects the field to sort results by                                                                                                 |
| `sortValues`        | Map of response field names to the `sortParam` values that sort by them, see [Pagination](#pagination)                                              |
| `directionParam`    | Parameter with the sort direction; required with `sortParam`                                                                                        |
| `ascendingValue`    | Value of `directionParam` for the ascending order; required with `sortParam`                                                                        |
| `descendingValue`   | Value of `directionParam` for the descending order; required with `sortParam`                                                                       |
| `parameterDefaults` | Values of parameters sent when the query doesn't filter on them, see [Parameter defaults](#parameter-defaults)                                      |

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
* the number of results reaches `maxResults`. Use it for servers that fail for
  results past a limit, like GitHub search, which returns at most 1,000 results
  with any page size;
* the number of results reaches the query's `LIMIT`, when Trino doesn't apply
  any other filter to the returned rows. With an `IN` list, every value is
  requested separately and reads up to the limit;
* a page has fewer results than the page size, when `limitParam` is set and its
  value is known, either from a query filter, from
  [`parameterDefaults`](#parameter-defaults), or from the parameter's default in
  the spec. The page size is limited by the parameter's `maximum`, if declared,
  because servers often return fewer results than requested when the number is
  too high. Servers can also return fewer results than the declared page size,
  so a short page ends pagination only when the response has no value at
  `totalResultsPath`, `totalPagesPath`, or `hasMorePath`.

Pagination never guesses from page contents. If none of these apply, for
example when the server returns the last page for every page number past it and
declares no page size or total, the query makes requests until it's cancelled.
Set `maxPages` for such servers.

A query with `ORDER BY` and `LIMIT` on a single column also stops pagination
after reading enough rows, when the column is in `sortValues`, so the server
returns the top rows first. The connector sends `sortParam` and `directionParam`
only when:

* the column is a number, boolean, `DATE`, or `TIMESTAMP`, or `sortValues`
  sets `binaryOrder` for it, because servers often sort text ignoring case or
  using a locale collation, while Trino compares code points;
* the field is required and not nullable in responses of all operations used
  for `SELECT`, because the server might place nulls differently than
  `NULLS FIRST` or `NULLS LAST` in the query;
* Trino doesn't apply any other filter to the returned rows;
* the query doesn't read the columns of `sortParam` and `directionParam`, and
  doesn't filter on them with different values.

Otherwise it requests all pages, and Trino sorts the rows. Declare only fields
that the server sorts the same way as Trino.

For other columns, like `VARCHAR`, the value in `sortValues` can be an object
with the `sortParam` value in `value`, and `binaryOrder: true`, if the server
sorts them the same way as Trino. For text, it means comparing code points, so
`Banana` comes before `apple`:

```yaml
x-trino:
  sortParam: "sort"
  sortValues:
    updated_at: "updated"
    login:
      value: "login"
      binaryOrder: true
```

Don't set it for a server that returns `apple`, `Banana`, `cherry`, sorting
them ignoring case, because `ORDER BY login LIMIT 1` would return `apple`
instead of `Banana`. Every operation declaring the field in `sortValues` must
set `binaryOrder` for it.

### Reading pages concurrently

By default, all pages are read one after another, in a single split. The
`split-strategy` catalog property, and the `splitStrategy` property in `x-trino`
for a single operation, can make the connector read pages concurrently, in
separate splits:

* `probe` - when `totalResultsPath` or `totalPagesPath` is set, the first page
  is read in a split right away, and the coordinator also requests it to get the
  total, and creates a split for every remaining page. The number of pages is
  the value at `totalPagesPath`, or the value at `totalResultsPath` divided by
  the number of results on the first page, which is full when more results
  follow, even if the server returns fewer results than the declared page
  size. If the first page has no total, the remaining pages are read
  in a single split. Results with a single page are requested twice. To make
  the extra request cheaper, set `probe-limit` or `probeLimit` to a small page
  size, like `0` or `1`, to request only the total number of results. It's
  used only when `limitParam` and `totalResultsPath` are set, and the regular
  page size is known, from a filter, from `parameterDefaults`, or from the
  parameter's default, because the totals of a smaller page can't tell the
  number of regular pages otherwise.
* `stride` - create a split for every worker node, or as many as set in
  `strideSplits`, without requesting any page on the coordinator. With N
  splits, the first split reads pages 1, N+1, 2N+1, and so on, the second
  split reads pages 2, N+2, 2N+2, and so on. Every split stops at the last page
  like a single split would, and skips pages past the last one, using
  `totalResultsPath` or `totalPagesPath`. Servers often return the last page
  for every page number past it, so the strategy is used only when
  `totalPagesPath` is set, or `totalResultsPath` is set and the page size is
  known, from a filter, from `parameterDefaults`, or from the parameter's
  default. With `maxResults`, the page size must also be known. When
  `totalResultsPath` or `totalPagesPath` is set, but the page size is not
  known, the `probe` strategy is used instead, which gets the page size from
  the first page. Otherwise, pages are read in a single split. Results with
  fewer pages than splits make extra requests for pages past the last one.
  With `totalResultsPath`, splits skip pages using the declared page size, so
  if the server returns fewer results per page, set `totalPagesPath` or use
  the `probe` strategy.
* `off` - read all pages in a single split.

Pages are read one after another when the query has a `LIMIT`, so pagination
can stop once it's reached. The `probe` strategy is not used when filters on
multiple values, like `IN` lists, already read every value in a separate split.
Splits are still limited by `maxPages`, `maxResults`, `max-requests-per-second`,
and `max-splits-per-second`. Rows from different pages can be returned in any
order.

The `split_strategy` and `probe_limit` catalog session properties override both
the spec and the catalog properties for a single query, for all its tables in
the catalog:

```sql
SET SESSION openapi.split_strategy = 'probe';
SET SESSION openapi.probe_limit = '0';
```

## Range filters

Many APIs accept parameters that limit the values of a response field, like
GitHub's `since` and `until`, which return only issues updated in a time range.
Their names don't say which field they limit, or whether they include the
given value, so the connector only uses them when `rangeFilters` maps them to a
response field:

```yaml
paths:
  /issues:
    get:
      parameters:
        - in: query
          name: since
          schema:
            type: string
            format: date-time
      # ...
      x-trino:
        rangeFilters:
          updated_at:
            lowerParam: since
            lowerInclusive: true
            exact: false
```

Then a query like this sends `since=2024-01-01T00:00:00.000Z`:

```sql
SELECT number, title FROM github.default.issues WHERE updated_at >= TIMESTAMP '2024-01-01 00:00:00';
```

The keys of `rangeFilters` are names of response fields, and the values have
these properties:

| Property         | Description                                                                                               |
| ---------------- | --------------------------------------------------------------------------------------------------------- |
| `lowerParam`     | Parameter with the lowest value of the field                                                              |
| `lowerInclusive` | `true` if the API returns rows equal to the `lowerParam` value, required with `lowerParam`                |
| `upperParam`     | Parameter with the highest value of the field                                                             |
| `upperInclusive` | `true` if the API returns rows equal to the `upperParam` value, required with `upperParam`                |
| `exact`          | `true` if the API returns only rows matching the bounds, so Trino doesn't filter them; `false` by default |

The parameters must be query, header, cookie, or body parameters of the same
operation, with the same type as the field. Their values are formatted as the
parameters declare, like any other filter. A bound is sent only when the API
returns all rows matching the query with it:

* `updated_at > X` sends an inclusive parameter with `X`, but not an exclusive
  one for `updated_at >= X`, because the API would skip rows equal to `X`.
  If you're not sure if the API includes the value, declare it as exclusive.
* Several ranges, like `x < 1 OR x > 5`, or `x IN (1, 5)`, are sent as one range
  from the lowest to the highest value, so they can't be sent if that range is
  unbounded.
* A value that loses precision when formatted, like a timestamp in a format
  without fractions of seconds, is sent only as a lower bound.
* Ranges that include `NULL` are not sent.
* A filter on the parameter itself, like `since = TIMESTAMP '2024-01-01'`, is
  sent instead of the bound.
* Bounds don't change which path is selected for a request, and paths without
  the bound parameters read all rows.

Trino still applies the filter to the returned rows, unless `exact` is `true`,
the bounds match the comparisons in the query, and every path of the table can
send them. Set `exact` only if the API compares the field values exactly like
SQL, with the same precision. For example, an API that ignores fractions of
seconds, or compares the time of the last change of a resource and not the
field, can return rows the query doesn't match, which would then not be
filtered out. Because Trino doesn't filter the rows of an exact range, a
`LIMIT` can stop pagination, like described in [Pagination](#pagination).

## Parameter defaults

The connector sends only the parameters that the query filters on, so for all
other parameters, the API uses its own defaults. They often return less data
than a SQL query expects. For example, GitHub lists only open issues and pull
requests, unless `state` is `all`, and returns 30 results per page, unless
`per_page` is set, so reading all pages needs more requests.

`parameterDefaults` maps names of parameters to values the connector sends when
the query doesn't filter on them:

```yaml
paths:
  /repos/{owner}/{repo}/pulls:
    get:
      # ...
      x-trino:
        pageParam: "page"
        limitParam: "per_page"
        parameterDefaults:
          state: "all"
          per_page: 100
```

Then this query reads closed pull requests too, 100 on every page:

```sql
SELECT number, title, state FROM github.default.repos_pulls WHERE owner = 'trinodb' AND repo = 'trino';
```

* The parameters must be optional query, header, or cookie parameters of the
  same operation, but not `pageParam`. Their schemas must be strings, integers,
  or booleans, and the values must match their type, `enum`, `minimum`, and
  `maximum`. Loading the spec fails otherwise.
* A filter on the parameter is sent instead of the default, including every
  value of an `IN` list and values of dynamic filters, and so is a bound of a
  [range filter](#range-filters). `ORDER BY` with `LIMIT` sends the sort
  parameters instead of their defaults.
* A default is sent only by the operation that declares it, so paths without the
  parameter, like getting a single item, don't send it.
* Rows read using the default have its value in the parameter's column, unless
  the response has a field with the same name. So a query without a filter
  returns the same rows as a query filtering on the default value, and Trino
  applies filters it can't send, like `LIKE`, to that value.
* The default value of `limitParam` is the page size that ends pagination, like
  described in [Pagination](#pagination). Don't change the parameter's `default`
  in its schema instead, because that's the value the server uses when the
  parameter is not sent.
* `INSERT`, `UPDATE`, and `DELETE` statements send the defaults too, when the
  statement doesn't set the parameter.

## Resource identifiers

Paths like `/pet/{petId}` identify a resource with a path parameter, and the
response usually has the same value in a field with another name, like `id`.
The connector can't tell that they hold the same value, so the table has two
columns, `pet_id` and `id`, and the value of `pet_id` is copied from the query
filter into every row, or is `NULL` when the query doesn't filter on it.

Set `field` in the `x-trino` extension of the path parameter to the name of the
response field it matches:

```yaml
paths:
  /pet/{petId}:
    get:
      parameters:
        - name: petId
          in: path
          required: true
          schema:
            type: integer
            format: int64
          x-trino:
            field: id
      # ...
```

Then the parameter and the field are a single column, named after the field. A
filter on it, like `WHERE id = 1`, uses the path with the parameter, and rows
have the value of the field from the response, also when the query reads a
list of resources without the filter:

```sql
SELECT id, name FROM openapi.default.pet WHERE id = 1;
```

* Only path parameters can be linked to a field.
* The field must be a field of the responses of the table, with the same type as
  the parameter. Loading the spec fails otherwise.
* Set `field` in every operation of the path that has the parameter, or once in
  the parameters of the path. Loading the spec fails if only some operations
  link the parameter.

## Error messages

When the server responds with an error, the query fails with a message that
contains the operation ID (or the path, if the operation has no ID), the HTTP
method, the URL without the query string, and the HTTP status code. It doesn't
contain the response body, because the body might not be readable, and it might
contain sensitive information. Set `errorPath` to include a readable message
from the response.

Most APIs report errors the same way in all operations, so `errorPath` can also
be set in the `x-trino` extension of the whole document. Operations use it,
unless they set their own:

```yaml
openapi: 3.0.3
x-trino:
  errorPath: "$response.body#/message"
paths:
  # ...
```

Failed requests have an `EXTERNAL` error type, so clients can tell them apart
from errors in the connector:

* `OPENAPI_HTTP_ERROR` (`0x0700_0000`) for error responses;
* `OPENAPI_CONNECTION_ERROR` (`0x0700_0001`) for network errors;
* `OPENAPI_INVALID_RESPONSE` (`0x0700_0002`) for responses that are not valid
  JSON, or don't have the shape described in the specification, like a string
  that is not a number in an `integer` property, or a date in a different
  format. The message names the column and the expected type, but not the
  value.

Only the first 64 KiB of an error response are read, and `errorPath` is ignored
for longer responses.

The `DEBUG` level of the `pl.net.was` logger logs every request, with the URL
without its query string, and the size of request and response bodies. The
bodies themselves, including error responses, are logged at the `DEBUG` level of
the separate `trino-openapi-bodies` logger, because they can contain credentials
and personal data:

```properties
trino-openapi-bodies=DEBUG
```

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
| [Cloudflare API](https://developers.cloudflare.com/api/)                                          | `CLOUDFLARE_API_TOKEN`                       |

The GitHub, Datadog, and Cloudflare catalogs use copies of the upstream specs,
included in the image, with `x-trino` extensions for pagination, or fixes for
values the OpenAPI parser rejects. See [`dev/specs`](dev/specs/README.md) for
how they're made.

# Limitations

* Every Trino node reads the specification when the catalog starts, so every
  node must be able to reach `spec-location`, and get the same specification
  ([#294](https://github.com/nineinchnick/trino-openapi/issues/294)). To use a
  changed specification, restart Trino.
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
* Only equality and `IN` filters, ranges expanded to single values, and ranges
  of fields with [range filters](#range-filters) are sent in requests. Other
  operators, like `LIKE`, are applied by Trino
  ([#5](https://github.com/nineinchnick/trino-openapi/issues/5)).
  `LIMIT` doesn't change the page size sent in requests, and it doesn't stop
  pagination when Trino filters the returned rows. `ORDER BY` is sent only for
  a single number, boolean, `DATE`, or `TIMESTAMP` column declared in
  `sortValues` that every response includes, or another column with
  `binaryOrder` set.
* Only page number pagination is supported, not offset or cursor pagination
  ([#96](https://github.com/nineinchnick/trino-openapi/issues/96)). Pages are
  requested one after another, unless a
  [split strategy](#reading-pages-concurrently) is set.
* `POST` and `PATCH` requests, other than `POST` operations used only for
  `SELECT`, are [retried](#retries) only after `429 Too Many Requests`, because
  repeating them can create duplicates or apply a change twice, so another
  temporary error in an `INSERT`, or an `UPDATE` that uses `PATCH`, fails the
  query.
* Only JSON request and response bodies are supported, not form-encoded or
  multipart bodies ([#250](https://github.com/nineinchnick/trino-openapi/issues/250)).
* Responses that are arrays of plain values
  ([#219](https://github.com/nineinchnick/trino-openapi/issues/219)), and objects
  with dynamic keys ([#172](https://github.com/nineinchnick/trino-openapi/issues/172)),
  are not mapped to rows. Some unions, like `anyOf` with a `null` alternative,
  are mapped to `VARCHAR` holding JSON, with quoted strings
  ([#52](https://github.com/nineinchnick/trino-openapi/issues/52)).
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
  applies when every request that can be used either sends it, or the connector
  applies it to the rows the request returns, the same way Trino would. A filter
  that can't be applied like that is still applied by Trino, even if it means
  reading more data.
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
