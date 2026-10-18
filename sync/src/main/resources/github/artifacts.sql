-- Metadata of artifacts of completed workflow runs from the last 2 months, at least 2 hours old,
-- so artifacts uploaded after the run completed are included. Contents of artifacts are not read.
CREATE SCHEMA IF NOT EXISTS ${target};

CREATE TABLE IF NOT EXISTS ${target}.sync_state (
    step varchar,
    owner varchar,
    repo varchar,
    parent_id bigint,
    version varchar,
    checked_at timestamp(6)
);

CREATE TABLE IF NOT EXISTS ${target}.artifacts AS
SELECT
    owner,
    repo,
    run_id,
    id,
    node_id,
    name,
    size_in_bytes,
    url,
    archive_download_url,
    expired,
    digest,
    created_at,
    expires_at,
    updated_at
FROM ${source}.repos_actions_runs_artifacts
WHERE owner = '${owner}' AND repo = '${repo}'
WITH NO DATA;

-- loop: batches
MERGE INTO ${target}.artifacts t
USING (
    WITH parents AS (
        SELECT id AS parent_id, CAST(max(run_attempt) AS varchar) AS version
        FROM ${target}.runs
        WHERE owner = '${owner}' AND repo = '${repo}' AND created_at > current_timestamp - INTERVAL '2' MONTH AND created_at < current_timestamp - INTERVAL '2' HOUR
        GROUP BY id
    ),
    checked AS (
        SELECT parent_id, max_by(version, checked_at) AS version
        FROM ${target}.sync_state
        WHERE step = 'artifacts' AND owner = '${owner}' AND repo = '${repo}'
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
            run_id,
            id,
            node_id,
            name,
            size_in_bytes,
            url,
            archive_download_url,
            expired,
            digest,
            created_at,
            expires_at,
            updated_at,
            row_number() OVER (PARTITION BY id) AS row_num
        FROM ${source}.repos_actions_runs_artifacts
        WHERE owner = '${owner}' AND repo = '${repo}' AND per_page = 100
            AND run_id IN (SELECT parent_id FROM batch)
    )
    WHERE row_num = 1
) s
ON t.id = s.id
WHEN NOT MATCHED THEN INSERT (
    owner,
    repo,
    run_id,
    id,
    node_id,
    name,
    size_in_bytes,
    url,
    archive_download_url,
    expired,
    digest,
    created_at,
    expires_at,
    updated_at
)
VALUES (
    s.owner,
    s.repo,
    s.run_id,
    s.id,
    s.node_id,
    s.name,
    s.size_in_bytes,
    s.url,
    s.archive_download_url,
    s.expired,
    s.digest,
    s.created_at,
    s.expires_at,
    s.updated_at
);

INSERT INTO ${target}.sync_state (step, owner, repo, parent_id, version, checked_at)
WITH parents AS (
    SELECT id AS parent_id, CAST(max(run_attempt) AS varchar) AS version
    FROM ${target}.runs
    WHERE owner = '${owner}' AND repo = '${repo}' AND created_at > current_timestamp - INTERVAL '2' MONTH AND created_at < current_timestamp - INTERVAL '2' HOUR
    GROUP BY id
),
checked AS (
    SELECT parent_id, max_by(version, checked_at) AS version
    FROM ${target}.sync_state
    WHERE step = 'artifacts' AND owner = '${owner}' AND repo = '${repo}'
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
SELECT 'artifacts', '${owner}', '${repo}', parent_id, version, CAST(current_timestamp AS timestamp(6))
FROM batch;
