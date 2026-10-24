-- Statistics of pull requests, like the number of changed lines, which are only returned when reading a single
-- pull request. Read for every version of a pull request, identified by the time it was last updated.
CREATE SCHEMA IF NOT EXISTS ${target};

CREATE TABLE IF NOT EXISTS ${target}.sync_state (
    step varchar,
    owner varchar,
    repo varchar,
    parent_id bigint,
    version varchar,
    checked_at timestamp(6)
);

CREATE TABLE IF NOT EXISTS ${target}.pull_stats AS
SELECT
    owner,
    repo,
    pull_number,
    comments,
    review_comments,
    commits,
    additions,
    deletions,
    changed_files,
    merged,
    mergeable_state,
    merged_by.id AS merged_by_id,
    merged_by.login AS merged_by_login,
    created_at,
    updated_at,
    closed_at,
    merged_at
FROM ${source}.repos_pulls
WHERE owner = '${owner}' AND repo = '${repo}'
WITH NO DATA;

-- loop: batches
MERGE INTO ${target}.pull_stats t
USING (
    WITH parents AS (
        SELECT number AS parent_id, CAST(updated_at AS varchar) AS version
        FROM ${target}.pulls
        WHERE owner = '${owner}' AND repo = '${repo}'
    ),
    checked AS (
        SELECT parent_id, max_by(version, checked_at) AS version
        FROM ${target}.sync_state
        WHERE step = 'pull_stats' AND owner = '${owner}' AND repo = '${repo}'
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
            pull_number,
            comments,
            review_comments,
            commits,
            additions,
            deletions,
            changed_files,
            merged,
            mergeable_state,
            merged_by.id AS merged_by_id,
            merged_by.login AS merged_by_login,
            created_at,
            updated_at,
            closed_at,
            merged_at,
            row_number() OVER (PARTITION BY pull_number, updated_at) AS row_num
        FROM ${source}.repos_pulls
        WHERE owner = '${owner}' AND repo = '${repo}'
            AND pull_number IN (SELECT parent_id FROM batch)
    )
    WHERE row_num = 1
) s
ON t.owner = s.owner AND t.repo = s.repo AND t.pull_number = s.pull_number AND t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (
    owner,
    repo,
    pull_number,
    comments,
    review_comments,
    commits,
    additions,
    deletions,
    changed_files,
    merged,
    mergeable_state,
    merged_by_id,
    merged_by_login,
    created_at,
    updated_at,
    closed_at,
    merged_at
)
VALUES (
    s.owner,
    s.repo,
    s.pull_number,
    s.comments,
    s.review_comments,
    s.commits,
    s.additions,
    s.deletions,
    s.changed_files,
    s.merged,
    s.mergeable_state,
    s.merged_by_id,
    s.merged_by_login,
    s.created_at,
    s.updated_at,
    s.closed_at,
    s.merged_at
);

INSERT INTO ${target}.sync_state (step, owner, repo, parent_id, version, checked_at)
WITH parents AS (
    SELECT number AS parent_id, CAST(updated_at AS varchar) AS version
    FROM ${target}.pulls
    WHERE owner = '${owner}' AND repo = '${repo}'
),
checked AS (
    SELECT parent_id, max_by(version, checked_at) AS version
    FROM ${target}.sync_state
    WHERE step = 'pull_stats' AND owner = '${owner}' AND repo = '${repo}'
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
SELECT 'pull_stats', '${owner}', '${repo}', parent_id, version, CAST(current_timestamp AS timestamp(6))
FROM batch;
