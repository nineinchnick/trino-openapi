-- Annotations of check runs from the last 2 months that have any.
CREATE SCHEMA IF NOT EXISTS ${target};

CREATE TABLE IF NOT EXISTS ${target}.sync_state (
    step varchar,
    owner varchar,
    repo varchar,
    parent_id bigint,
    version varchar,
    checked_at timestamp(6)
);

CREATE TABLE IF NOT EXISTS ${target}.check_run_annotations AS
SELECT
    owner,
    repo,
    check_run_id,
    path,
    start_line,
    end_line,
    start_column,
    end_column,
    annotation_level,
    title,
    replace(message, U&'\0000', ' ') AS message,
    replace(raw_details, U&'\0000', ' ') AS raw_details,
    blob_href
FROM ${source}.repos_check_runs_annotations
WHERE owner = '${owner}' AND repo = '${repo}'
WITH NO DATA;

-- loop: batches
MERGE INTO ${target}.check_run_annotations t
USING (
    WITH parents AS (
        SELECT id AS parent_id, CAST(annotations_count AS varchar) AS version
        FROM ${target}.check_runs
        WHERE owner = '${owner}' AND repo = '${repo}' AND annotations_count > 0 AND started_at > current_timestamp - INTERVAL '2' MONTH
    ),
    checked AS (
        SELECT parent_id, max_by(version, checked_at) AS version
        FROM ${target}.sync_state
        WHERE step = 'check_run_annotations' AND owner = '${owner}' AND repo = '${repo}'
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
            check_run_id,
            path,
            start_line,
            end_line,
            start_column,
            end_column,
            annotation_level,
            title,
            replace(message, U&'\0000', ' ') AS message,
            replace(raw_details, U&'\0000', ' ') AS raw_details,
            blob_href,
            row_number() OVER (PARTITION BY check_run_id, path, start_line, end_line, start_column, end_column, title, message) AS row_num
        FROM ${source}.repos_check_runs_annotations
        WHERE owner = '${owner}' AND repo = '${repo}' AND per_page = 100
            AND check_run_id IN (SELECT parent_id FROM batch)
    )
    WHERE row_num = 1
) s
ON t.check_run_id = s.check_run_id AND t.path IS NOT DISTINCT FROM s.path AND t.start_line IS NOT DISTINCT FROM s.start_line AND t.end_line IS NOT DISTINCT FROM s.end_line AND t.start_column IS NOT DISTINCT FROM s.start_column AND t.end_column IS NOT DISTINCT FROM s.end_column AND t.title IS NOT DISTINCT FROM s.title AND t.message IS NOT DISTINCT FROM s.message
WHEN NOT MATCHED THEN INSERT (
    owner,
    repo,
    check_run_id,
    path,
    start_line,
    end_line,
    start_column,
    end_column,
    annotation_level,
    title,
    message,
    raw_details,
    blob_href
)
VALUES (
    s.owner,
    s.repo,
    s.check_run_id,
    s.path,
    s.start_line,
    s.end_line,
    s.start_column,
    s.end_column,
    s.annotation_level,
    s.title,
    s.message,
    s.raw_details,
    s.blob_href
);

INSERT INTO ${target}.sync_state (step, owner, repo, parent_id, version, checked_at)
WITH parents AS (
    SELECT id AS parent_id, CAST(annotations_count AS varchar) AS version
    FROM ${target}.check_runs
    WHERE owner = '${owner}' AND repo = '${repo}' AND annotations_count > 0 AND started_at > current_timestamp - INTERVAL '2' MONTH
),
checked AS (
    SELECT parent_id, max_by(version, checked_at) AS version
    FROM ${target}.sync_state
    WHERE step = 'check_run_annotations' AND owner = '${owner}' AND repo = '${repo}'
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
SELECT 'check_run_annotations', '${owner}', '${repo}', parent_id, version, CAST(current_timestamp AS timestamp(6))
FROM batch;
