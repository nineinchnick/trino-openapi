Sync
====

A small program that keeps an incremental copy of data read with the OpenAPI
connector in another Trino catalog, like Iceberg, for archiving and analysis.
It runs SQL scripts, called steps, through the Trino JDBC driver. Each step
copies one table, and reads only data that changed since the previous run.

The bundled steps copy GitHub data of a repository from a catalog using the
[bundled GitHub spec](../specs/github.json). They replace the `Sync` class of
[trino-rest](https://github.com/nineinchnick/trino-rest).

The program knows nothing about GitHub. Everything specific to an API, like
which parameters to send, how to find new data, and which parents to read
children for, is in the SQL scripts, so you can change them or write your own.

# Running

Build the program:

```bash
./mvnw install -DskipTests
./mvnw -f sync/pom.xml package
```

The first command installs the connector, which the tests of the program use.

You need a Trino server with:

* a catalog using the OpenAPI connector with the GitHub spec, like the
  `github` catalog of the [Docker image](../README.md#bundled-catalogs). Use a
  token that can only read data, because the connector can also change data
  with `INSERT`, `UPDATE`, and `DELETE`;
* a catalog that supports `MERGE`, like Iceberg, where the copy is saved.

Then run:

```bash
export SYNC_TRINO_URL=jdbc:trino://localhost:8080
export SYNC_SOURCE=github.default
export SYNC_TARGET=iceberg.github
export SYNC_OWNER=nineinchnick
export SYNC_REPO=trino-openapi
java -jar sync/target/trino-openapi-sync-*-executable.jar
```

The program runs every step, even if some fail, and exits with a non-zero code
if any step failed. Run it on a schedule, for example every hour.

# Configuration

Settings are read from environment variables with the `SYNC_` prefix, and from
a properties file passed as the first argument, which uses the same names
without the prefix, in lower case, like `trino_url`. Environment variables
override the file.

| Environment variable      | Default                       | Description                                                                                      |
| ------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------ |
| `SYNC_TRINO_URL`          | `jdbc:trino://localhost:8080` | [JDBC URL](https://trino.io/docs/current/client/jdbc.html) of the Trino server                   |
| `SYNC_TRINO_USER`         | `sync`                        | Trino user                                                                                       |
| `SYNC_TRINO_PASSWORD`     |                               | Trino password                                                                                   |
| `SYNC_SESSION_PROPERTIES` |                               | Session properties, like `github.http_cache_enabled:false`, separated by semicolons              |
| `SYNC_STEPS`              | steps listed in `steps.txt`   | Comma-separated names of the steps to run, in order                                              |
| `SYNC_STEPS_DIR`          | bundled GitHub steps          | Directory with the scripts of the steps, and a `steps.txt` file listing the default ones         |
| `SYNC_MAX_RETRIES`        | `3`                           | Maximum number of times a statement that failed with an error that might be transient is retried |
| `SYNC_RETRY_DELAY`        | `10s`                         | Delay before the first retry, doubled for every next retry                                       |
| `SYNC_MAX_ITERATIONS`     | `1000`                        | Maximum number of iterations of a loop, after which the step fails                               |

All other settings are parameters of the scripts. The GitHub steps use:

| Environment variable | Description                                                                                                           |
| -------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `SYNC_SOURCE`        | Catalog and schema with the GitHub tables, like `github.default`                                                      |
| `SYNC_TARGET`        | Catalog and schema where the copy is saved, like `iceberg.github`; created if it doesn't exist                        |
| `SYNC_OWNER`         | Owner of the repository, a user or an organization                                                                    |
| `SYNC_REPO`          | Name of the repository                                                                                                |
| `SYNC_BATCH_SIZE`    | Number of parents, like pull requests, to read children for in one statement; `30` by default, at most 256, see below |

The JDBC connection uses the UTC time zone, because GitHub returns times in UTC,
and steps compare them with the current time.

## Retries and rate limits

The connector retries requests that failed with a transient error, see
[Retries](../README.md#retries). The program also retries a whole statement if
it failed with any error not caused by the query itself, like a network error,
a failed Iceberg commit, or a failed request, after waiting `SYNC_RETRY_DELAY`,
then twice as long, and so on. All statements of the bundled steps are safe to
repeat.

When the GitHub rate limit is used up, GitHub responds with `403 Forbidden`
without a `Retry-After` header, and the query fails. The limit resets within an
hour, so either set `SYNC_RETRY_DELAY` high enough, like `10m`, or limit the
number of requests with `max-requests-per-second` in the catalog, which is the
better option, because it also leaves some of the limit for other clients.
Waiting until the limit resets, using the rate limit headers GitHub sends,
needs a separate change in the connector; until then, the statement retries
and `max-requests-per-second` are the only protection. The first run of a big repository makes many requests, because it reads all
the data.

## HTTP cache

Every run reads some pages and parents again, which haven't changed, like the
first page of pull requests, or the newest issue at the cursor. Set
[`http-cache.directory`](../README.md#http-cache) in the source catalog, and
`SYNC_SESSION_PROPERTIES=github.http_cache_enabled:false`, where `github` is the
name of the catalog. Then the connector sends conditional requests for responses
it has cached, and GitHub responds with `304 Not Modified` for unchanged ones,
which it doesn't count against the rate limit.

Without the session property, the connector also reuses cached responses
without asking GitHub while they're fresh. GitHub marks responses of list
endpoints, like `/repos/{owner}/{repo}/pulls`, as fresh for 60 seconds
(`Cache-Control: private, max-age=60`), so a run could read data up to a minute
old. A cursor would then just stop before the changes, and the next run would
read them, but a parent saved in `sync_state` with a newer version than its
cached children would not be read again until it changes again.

# Steps

A step is a SQL script, in a file named like the step, with statements
separated by semicolons. Values of parameters are inserted in place of
`${name}`, or `${name:default}` when the parameter has a default value, without
quoting, because they're also used for names of catalogs and schemas.

Statements run once, in order, unless they follow a loop directive:

```sql
CREATE TABLE IF NOT EXISTS ${target}.items AS SELECT ... WITH NO DATA;

-- loop: pages
-- stop-after: 2
MERGE INTO ${target}.items t
USING (SELECT ... FROM ${source}.items WHERE page = ${page}) s
...;
```

Statements after the loop directive run repeatedly, until the last one of them
changes no rows in `stop-after` iterations in a row, which is 1 by default.
The `pages` loop sets the `page` parameter to 1, 2, and so on. The `batches`
loop doesn't set any parameters, and is used to process a limited number of
parents in every iteration.

## How the GitHub steps find new data

The steps use the features of the connector instead of reading everything:

* **Cursors.** Endpoints with a `since` parameter, like issues and comments,
  are read with a filter like
  `updated_at >= (SELECT CAST(max(updated_at) AS timestamp(3)) FROM ${target}.issues ...)`.
  Trino passes the result of the subquery to the connector as a dynamic filter,
  which sends it as the `since` parameter, because the spec declares it in
  `rangeFilters`. The cast is required: Iceberg saves timestamps with
  microseconds, and a dynamic filter is only created when the types match.
  Without the cast the query still returns correct results, but reads all
  pages. When the target table is empty, the cursor is `2000-01-01`, because
  GitHub returns no results for `since` set to the start of 1970.
* **Sorted pages.** Pull requests and repositories can't be filtered by update
  time, but can be sorted by it, so the steps read one page at a time, filtering
  on `page`, `sort = 'updated'` and `direction = 'desc'`, until a page has no
  changes. Workflow runs can't be sorted, but GitHub returns the newest first,
  so the step reads pages until two pages in a row have no new runs. A new
  attempt of an older run, which is further than that, is missed, like in the
  trino-rest Sync, and so are its jobs. To read more pages, raise the
  `stop-after` value in [`runs.sql`](src/main/resources/github/runs.sql).
* **Parents and children.** Children, like the commits of a pull request, are
  read with a filter like `pull_number IN (SELECT parent_id FROM batch)`. Trino
  passes the values to the connector as a dynamic filter, and the connector
  makes a request for every value. The batch is limited to `${batch_size:30}`
  parents, because the connector only waits 20 seconds for dynamic filters, and
  accepts at most `domain-expansion-limit` (256) values. Parents that were read
  are saved in the `sync_state` table, with a version, like the time the parent
  was last updated, so they're read again only when they change, and parents
  without any children aren't read again.
* **Every parameter is explicit.** The connector only sends parameters the query
  filters on, so GitHub defaults would apply otherwise. The steps filter on
  `state = 'all'` for issues and pull requests, `filter = 'all'` for jobs and
  check runs, and `per_page = 100` everywhere, to make fewer requests.

Rows are saved with `MERGE`, keyed by the identifier of the row, and the source
rows are deduplicated first, because a row can be returned on two pages when
the data changes during the sync. Updated rows replace the old ones, except in
tables that keep a history, as described below.

| Table                   | Source table                                  | Strategy                                                        | Parent                                                              | Read again                                           |
| ----------------------- | --------------------------------------------- | --------------------------------------------------------------- | ------------------------------------------------------------------- | ---------------------------------------------------- |
| `repos`                 | `users_repos`                                 | pages sorted by update time, update by `id`                     |                                                                     |                                                      |
| `org_repos`             | `orgs_repos`                                  | pages sorted by update time, update by `id` in `repos`          |                                                                     |                                                      |
| `commits`               | `repos_commits`                               | cursor on the commit date, sent as `since`, insert by `sha`     |                                                                     |                                                      |
| `issues`                | `repos_issues`                                | cursor on `updated_at`, update by `id`                          |                                                                     |                                                      |
| `issue_comments`        | `repos_owner_repo_issues_comments_comment_id` | cursor on `updated_at`, update by `id`                          |                                                                     |                                                      |
| `pulls`                 | `repos_pulls`                                 | pages sorted by update time, update by `id`                     |                                                                     |                                                      |
| `pull_commits`          | `repos_pulls_commits`                         | batches, insert by `pull_number`, `sha`                         | `pulls.number`                                                      | when the pull request was updated in the last 3 days |
| `pull_stats`            | `repos_pulls`, one pull request at a time     | batches, insert by `pull_number`, `updated_at`                  | `pulls.number`                                                      | when the pull request was updated                    |
| `review_comments`       | `repos_owner_repo_pulls_comments_comment_id`  | cursor on `updated_at`, update by `id`                          |                                                                     |                                                      |
| `reviews`               | `repos_pulls_reviews`                         | batches, update by `id`                                         | `pulls.number`                                                      | when the pull request was updated                    |
| `workflows`             | `repos_actions_workflows`                     | all rows, update by `id`                                        |                                                                     |                                                      |
| `runs`                  | `repos_actions_runs`                          | pages of completed runs, insert by `id`, `run_attempt`          |                                                                     |                                                      |
| `jobs`                  | `repos_actions_runs_jobs`                     | batches, insert by `id`                                         | `runs.id`, completed in the last 2 months                           | when the run has a new attempt                       |
| `steps`                 | `jobs.steps` in the target                    | jobs from the last 2 months, insert by `job_id`, `number`       |                                                                     |                                                      |
| `artifacts`             | `repos_actions_runs_artifacts`                | batches, insert by `id`                                         | `runs.id`, from the last 2 months, at least 2 hours old             | when the run has a new attempt                       |
| `check_suites`          | `repos_check_suites`                          | batches, update by `id`                                         | `runs.check_suite_id`, from the last 15 days, at least 2 hours old  | when the run has a new attempt                       |
| `check_runs`            | `repos_check_suites_check_runs`               | batches, update by `id`                                         | `runs.check_suite_id`, from the last 2 months, at least 2 hours old | when the run has a new attempt                       |
| `check_run_annotations` | `repos_check_runs_annotations`                | batches, insert by `check_run_id`, position, title, and message | `check_runs.id` with annotations, from the last 2 months            | when the number of annotations changes               |
| `teams`                 | `orgs_teams`                                  | snapshot, sets `created_at` and `removed_at`                    |                                                                     |                                                      |
| `members`               | `orgs_members`, `orgs_teams_members`          | snapshot, sets `joined_at` and `removed_at`                     | `orgs_teams.slug`                                                   |                                                      |
| `collaborators`         | `repos_collaborators`                         | snapshot, sets `joined_at` and `removed_at`                     |                                                                     |                                                      |

The steps run by default are listed in
[`steps.txt`](src/main/resources/github/steps.txt). `pull_stats` and
`artifacts` aren't, because they make a request for every pull request or run;
`org_repos`, `teams`, and `members` need an organization as the owner, and
`collaborators` needs a token with push access to the repository.

The `repos` step reads `/users/{owner}/repos`, which works for users and
organizations, but only returns public repositories of organizations. When the
owner is an organization, also run `org_repos`, which reads
`/orgs/{owner}/repos` with `type = 'all'`, so it includes private repositories
the token can read, and saves them in the same `repos` table. It's a separate
step, because GitHub responds with `404 Not Found` for users, and the program
doesn't need to know what kind of owner it syncs.

The tables have a selection of columns, with nested objects flattened, like
`user_login`, and arrays of objects reduced to arrays of names or numbers, like
`label_names`. Some nested objects can't be saved in Iceberg as they are, like
`reactions`, which has fields named `+1` and `-1`. Timestamps are in UTC.

## Differences from the trino-rest Sync

* Job logs and contents of artifacts are not copied, because they're not JSON.
  The `artifacts` step saves their metadata.
* Repositories of users and organizations are read by separate steps, `repos`
  and `org_repos`, instead of one that reads both.
* Tables have different columns than the ones of trino-rest, and are updated
  instead of keeping every version of a row, except `pull_stats` and `runs`.
* Review comments and issue comments don't fall back to reading comments of
  every issue when reading them with `since` fails.
* There's no delay between queries (`QUERY_DELAY_MS`); limit the requests in
  the catalog instead.

# Testing

The tests run the program against a Trino server started in the same JVM, with
the OpenAPI connector reading from a small fake GitHub server, and an Iceberg
catalog saving data in a temporary directory. One test syncs a few tables of
this repository from the real GitHub API, if the `GITHUB_TOKEN` environment
variable is set.

```bash
./mvnw install -DskipTests
./mvnw -f sync/pom.xml verify
```

The tests use the connector installed in the local Maven repository. This
project isn't released with the connector, so its version doesn't change when
the connector's does. After a release, or to test another build of the
connector, set its version with the `dep.trino-openapi.version` property, like
CI does:

```bash
./mvnw -f sync/pom.xml verify -Ddep.trino-openapi.version=$(./mvnw -q help:evaluate -Dexpression=project.version -DforceStdout)
```
