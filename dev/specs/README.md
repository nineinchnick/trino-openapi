# Patched OpenAPI specs

Some specs used by the bundled catalogs and the tests are upstream specs with
changes, like `x-trino` extensions that describe pagination. Each change is
kept here as a [JSON Merge Patch](https://www.rfc-editor.org/rfc/rfc7386)
document, so the patched copy can be regenerated from a newer upstream version.

A merge patch merges objects recursively, removes members set to `null`, and
replaces any other value. Arrays are always replaced as a whole, so changing
one element requires the whole array in the patch.

Patches are applied with:

* [`merge-patch.jq`](merge-patch.jq) for JSON specs, using `jq` 1.7 or newer,
  which keeps number literals unchanged,
* [`merge-patch.yq`](merge-patch.yq) for YAML specs, using
  [`yq`](https://github.com/mikefarah/yq) 4.

Run the commands from the repository root. Unpatched copies are formatted with
`jq --indent 2 .`, so they can be compared with the patched ones.

## GitHub

* Upstream: https://github.com/github/rest-api-description/raw/main/descriptions/api.github.com/api.github.com.json
* Patch: [`github.patch.json`](github.patch.json)
* Output: `specs/github.json`, and the unpatched `src/test/resources/github.json`

```shell
curl -fsSL -o /tmp/github.json https://github.com/github/rest-api-description/raw/main/descriptions/api.github.com/api.github.com.json
jq --indent 2 . /tmp/github.json > src/test/resources/github.json
jq --indent 2 --slurpfile patch dev/specs/github.patch.json -f dev/specs/merge-patch.jq /tmp/github.json > specs/github.json
```

## Datadog

* Upstream: https://raw.githubusercontent.com/DataDog/datadog-api-client-java/master/.generator/schemas/v2/openapi.yaml
* Patch: [`datadog.patch.json`](datadog.patch.json)
* Output: `specs/datadog.yaml`

The upstream spec describes pagination in `x-pagination` extensions, which the
patch replaces with `x-trino`, so the connector reads rows from `resultsPath`.
Datadog numbers pages from 0, unless `pageStart` is set, and the connector
numbers pages from 1, so the patch keeps `pageParam` only where `pageStart`
is 1. Paginated `POST` operations are searches, so the patch marks them with
`sqlOperation: select`. `yq` changes the formatting of some values, like quoting and emoji
escapes, without changing them.

```shell
curl -fsSL -o /tmp/datadog.yaml https://raw.githubusercontent.com/DataDog/datadog-api-client-java/master/.generator/schemas/v2/openapi.yaml
yq eval-all --from-file dev/specs/merge-patch.yq /tmp/datadog.yaml dev/specs/datadog.patch.json > specs/datadog.yaml
```

## Cloudflare

* Upstream: https://raw.githubusercontent.com/cloudflare/api-schemas/main/openapi.json
* Patch: [`cloudflare.patch.json`](cloudflare.patch.json)
* Output: `specs/cloudflare.json`

The OpenAPI parser reports errors for some values in the upstream spec, and the
connector refuses to load a spec with errors. The patch removes them: multipart
encodings of parts that aren't properties of the request body schema, and
defaults that don't match the type, like `"200"` for an array, or
`"Now + 30 minutes"` for a date-time.

```shell
curl -fsSL -o /tmp/cloudflare.json https://raw.githubusercontent.com/cloudflare/api-schemas/main/openapi.json
jq --indent 2 --slurpfile patch dev/specs/cloudflare.patch.json -f dev/specs/merge-patch.jq /tmp/cloudflare.json > specs/cloudflare.json
```

## Unpatched copies

These test resources are upstream specs without changes. JSON specs are
formatted like the GitHub one, and YAML specs are saved as they are, for
example:

```shell
curl -fsSL https://developer.atlassian.com/cloud/jira/platform/swagger-v3.v3.json | jq --indent 2 . > src/test/resources/jira.json
```

| Output                             | Upstream                                                                          |
| ---------------------------------- | --------------------------------------------------------------------------------- |
| `src/test/resources/jira.json`     | https://developer.atlassian.com/cloud/jira/platform/swagger-v3.v3.json            |
| `src/test/resources/galaxy.json`   | https://galaxy.starburst.io/public/openapi/v1/json                                |
| `src/test/resources/petstore.yaml` | https://petstore3.swagger.io/api/v3/openapi.yaml                                  |
| `src/test/resources/openmeteo.yml` | https://raw.githubusercontent.com/open-meteo/open-meteo/main/openapi/forecast.yml |
