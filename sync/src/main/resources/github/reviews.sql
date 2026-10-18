-- Reviews of pull requests, read for batches of pull requests that weren't checked yet, or were updated after that.
CREATE SCHEMA IF NOT EXISTS ${target};

CREATE TABLE IF NOT EXISTS ${target}.sync_state (
    step varchar,
    owner varchar,
    repo varchar,
    parent_id bigint,
    version varchar,
    checked_at timestamp(6)
);

CREATE TABLE IF NOT EXISTS ${target}.reviews AS
SELECT
    owner,
    repo,
    id,
    node_id,
    pull_number,
    user.id AS user_id,
    user.login AS user_login,
    body,
    state,
    html_url,
    submitted_at,
    commit_id,
    author_association
FROM ${source}.repos_pulls_reviews
WHERE owner = '${owner}' AND repo = '${repo}'
WITH NO DATA;

-- loop: batches
MERGE INTO ${target}.reviews t
USING (
    WITH parents AS (
        SELECT number AS parent_id, CAST(updated_at AS varchar) AS version
        FROM ${target}.pulls
        WHERE owner = '${owner}' AND repo = '${repo}'
    ),
    checked AS (
        SELECT parent_id, max_by(version, checked_at) AS version
        FROM ${target}.sync_state
        WHERE step = 'reviews' AND owner = '${owner}' AND repo = '${repo}'
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
            node_id,
            pull_number,
            user.id AS user_id,
            user.login AS user_login,
            body,
            state,
            html_url,
            submitted_at,
            commit_id,
            author_association,
            row_number() OVER (PARTITION BY id) AS row_num
        FROM ${source}.repos_pulls_reviews
        WHERE owner = '${owner}' AND repo = '${repo}' AND per_page = 100
            AND pull_number IN (SELECT parent_id FROM batch)
    )
    WHERE row_num = 1
) s
ON t.id = s.id
WHEN MATCHED AND (s.state <> t.state OR s.body IS DISTINCT FROM t.body) THEN UPDATE SET
    owner = s.owner,
    repo = s.repo,
    id = s.id,
    node_id = s.node_id,
    pull_number = s.pull_number,
    user_id = s.user_id,
    user_login = s.user_login,
    body = s.body,
    state = s.state,
    html_url = s.html_url,
    submitted_at = s.submitted_at,
    commit_id = s.commit_id,
    author_association = s.author_association
WHEN NOT MATCHED THEN INSERT (
    owner,
    repo,
    id,
    node_id,
    pull_number,
    user_id,
    user_login,
    body,
    state,
    html_url,
    submitted_at,
    commit_id,
    author_association
)
VALUES (
    s.owner,
    s.repo,
    s.id,
    s.node_id,
    s.pull_number,
    s.user_id,
    s.user_login,
    s.body,
    s.state,
    s.html_url,
    s.submitted_at,
    s.commit_id,
    s.author_association
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
    WHERE step = 'reviews' AND owner = '${owner}' AND repo = '${repo}'
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
SELECT 'reviews', '${owner}', '${repo}', parent_id, version, CAST(current_timestamp AS timestamp(6))
FROM batch;
