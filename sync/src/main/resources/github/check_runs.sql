-- Check runs of check suites of completed workflow runs from the last 2 months, at least 2 hours old, so they're completed.
CREATE SCHEMA IF NOT EXISTS ${target};

CREATE TABLE IF NOT EXISTS ${target}.sync_state (
    step varchar,
    owner varchar,
    repo varchar,
    parent_id bigint,
    version varchar,
    checked_at timestamp(6)
);

CREATE TABLE IF NOT EXISTS ${target}.check_runs AS
SELECT
    owner,
    repo,
    id,
    check_suite_id,
    head_sha,
    node_id,
    external_id,
    url,
    html_url,
    details_url,
    status,
    conclusion,
    started_at,
    completed_at,
    output.title AS output_title,
    output.summary AS output_summary,
    output.text AS output_text,
    output.annotations_count AS annotations_count,
    name,
    app.id AS app_id,
    app.slug AS app_slug,
    transform(pull_requests, pull -> pull.number) AS pull_request_numbers
FROM ${source}.repos_check_suites_check_runs
WHERE owner = '${owner}' AND repo = '${repo}'
WITH NO DATA;

-- loop: batches
MERGE INTO ${target}.check_runs t
USING (
    WITH parents AS (
        SELECT check_suite_id AS parent_id, CAST(max(run_attempt) AS varchar) AS version
        FROM ${target}.runs
        WHERE owner = '${owner}' AND repo = '${repo}' AND check_suite_id IS NOT NULL
            AND created_at > current_timestamp - INTERVAL '2' MONTH
            AND created_at < current_timestamp - INTERVAL '2' HOUR
        GROUP BY check_suite_id
    ),
    checked AS (
        SELECT parent_id, max_by(version, checked_at) AS version
        FROM ${target}.sync_state
        WHERE step = 'check_runs' AND owner = '${owner}' AND repo = '${repo}'
        GROUP BY parent_id
    ),
    batch AS (
        SELECT p.parent_id, p.version
        FROM parents p
        LEFT JOIN checked c ON c.parent_id = p.parent_id
        WHERE c.parent_id IS NULL OR c.version <> p.version
        ORDER BY p.parent_id
        LIMIT ${batch_size:30}
    )
    SELECT *
    FROM (
        SELECT
            owner,
            repo,
            id,
            check_suite_id,
            head_sha,
            node_id,
            external_id,
            url,
            html_url,
            details_url,
            status,
            conclusion,
            started_at,
            completed_at,
            output.title AS output_title,
            output.summary AS output_summary,
            output.text AS output_text,
            output.annotations_count AS annotations_count,
            name,
            app.id AS app_id,
            app.slug AS app_slug,
            transform(pull_requests, pull -> pull.number) AS pull_request_numbers,
            row_number() OVER (PARTITION BY id) AS row_num
        FROM ${source}.repos_check_suites_check_runs
        WHERE owner = '${owner}' AND repo = '${repo}' AND filter = 'all' AND per_page = 100
            AND check_suite_id IN (SELECT parent_id FROM batch)
    )
    WHERE row_num = 1
) s
ON t.id = s.id
WHEN MATCHED AND (s.status <> t.status OR s.conclusion IS DISTINCT FROM t.conclusion) THEN UPDATE SET
    owner = s.owner,
    repo = s.repo,
    id = s.id,
    check_suite_id = s.check_suite_id,
    head_sha = s.head_sha,
    node_id = s.node_id,
    external_id = s.external_id,
    url = s.url,
    html_url = s.html_url,
    details_url = s.details_url,
    status = s.status,
    conclusion = s.conclusion,
    started_at = s.started_at,
    completed_at = s.completed_at,
    output_title = s.output_title,
    output_summary = s.output_summary,
    output_text = s.output_text,
    annotations_count = s.annotations_count,
    name = s.name,
    app_id = s.app_id,
    app_slug = s.app_slug,
    pull_request_numbers = s.pull_request_numbers
WHEN NOT MATCHED THEN INSERT (
    owner,
    repo,
    id,
    check_suite_id,
    head_sha,
    node_id,
    external_id,
    url,
    html_url,
    details_url,
    status,
    conclusion,
    started_at,
    completed_at,
    output_title,
    output_summary,
    output_text,
    annotations_count,
    name,
    app_id,
    app_slug,
    pull_request_numbers
)
VALUES (
    s.owner,
    s.repo,
    s.id,
    s.check_suite_id,
    s.head_sha,
    s.node_id,
    s.external_id,
    s.url,
    s.html_url,
    s.details_url,
    s.status,
    s.conclusion,
    s.started_at,
    s.completed_at,
    s.output_title,
    s.output_summary,
    s.output_text,
    s.annotations_count,
    s.name,
    s.app_id,
    s.app_slug,
    s.pull_request_numbers
);

INSERT INTO ${target}.sync_state (step, owner, repo, parent_id, version, checked_at)
WITH parents AS (
    SELECT check_suite_id AS parent_id, CAST(max(run_attempt) AS varchar) AS version
    FROM ${target}.runs
    WHERE owner = '${owner}' AND repo = '${repo}' AND check_suite_id IS NOT NULL
        AND created_at > current_timestamp - INTERVAL '2' MONTH
        AND created_at < current_timestamp - INTERVAL '2' HOUR
    GROUP BY check_suite_id
),
checked AS (
    SELECT parent_id, max_by(version, checked_at) AS version
    FROM ${target}.sync_state
    WHERE step = 'check_runs' AND owner = '${owner}' AND repo = '${repo}'
    GROUP BY parent_id
),
batch AS (
    SELECT p.parent_id, p.version
    FROM parents p
    LEFT JOIN checked c ON c.parent_id = p.parent_id
    WHERE c.parent_id IS NULL OR c.version <> p.version
    ORDER BY p.parent_id
    LIMIT ${batch_size:30}
)
SELECT 'check_runs', '${owner}', '${repo}', parent_id, version, CAST(current_timestamp AS timestamp(6))
FROM batch;
