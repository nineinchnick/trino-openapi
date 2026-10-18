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
