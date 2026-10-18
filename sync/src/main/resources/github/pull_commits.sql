-- Commits of pull requests, read for batches of pull requests that weren't checked yet,
-- or were updated after they were checked, in the last 3 days, when new commits might have been pushed.
-- Checked pull requests are saved in sync_state, so ones without commits aren't checked again.
CREATE SCHEMA IF NOT EXISTS ${target};

CREATE TABLE IF NOT EXISTS ${target}.sync_state (
    step varchar,
    owner varchar,
    repo varchar,
    parent_id bigint,
    version varchar,
    checked_at timestamp(6)
);

CREATE TABLE IF NOT EXISTS ${target}.pull_commits AS
SELECT
    owner,
    repo,
    sha,
    pull_number,
    commit.message AS commit_message,
    commit.tree.sha AS commit_tree_sha,
    commit.comment_count AS commit_comment_count,
    commit.verification.verified AS commit_verified,
    commit.verification.reason AS commit_verification_reason,
    commit.author.name AS author_name,
    commit.author.email AS author_email,
    commit.author.date AS author_date,
    author.id AS author_id,
    author.login AS author_login,
    commit.committer.name AS committer_name,
    commit.committer.email AS committer_email,
    commit.committer.date AS committer_date,
    committer.id AS committer_id,
    committer.login AS committer_login,
    transform(parents, parent -> parent.sha) AS parent_shas
FROM ${source}.repos_pulls_commits
WHERE owner = '${owner}' AND repo = '${repo}'
WITH NO DATA;

-- loop: batches
MERGE INTO ${target}.pull_commits t
USING (
    WITH parents AS (
        SELECT number AS parent_id, CAST(updated_at AS varchar) AS version, updated_at
        FROM ${target}.pulls
        WHERE owner = '${owner}' AND repo = '${repo}'
    ),
    checked AS (
        SELECT parent_id, max_by(version, checked_at) AS version
        FROM ${target}.sync_state
        WHERE step = 'pull_commits' AND owner = '${owner}' AND repo = '${repo}'
        GROUP BY parent_id
    ),
    batch AS (
        SELECT p.parent_id, p.version
        FROM parents p
        LEFT JOIN checked c ON c.parent_id = p.parent_id
        WHERE c.parent_id IS NULL OR (c.version <> p.version AND p.updated_at > current_timestamp - INTERVAL '3' DAY)
        ORDER BY p.parent_id
        LIMIT ${batch_size:30}
    )
    SELECT *
    FROM (
        SELECT
            owner,
            repo,
            sha,
            pull_number,
            commit.message AS commit_message,
            commit.tree.sha AS commit_tree_sha,
            commit.comment_count AS commit_comment_count,
            commit.verification.verified AS commit_verified,
            commit.verification.reason AS commit_verification_reason,
            commit.author.name AS author_name,
            commit.author.email AS author_email,
            commit.author.date AS author_date,
            author.id AS author_id,
            author.login AS author_login,
            commit.committer.name AS committer_name,
            commit.committer.email AS committer_email,
            commit.committer.date AS committer_date,
            committer.id AS committer_id,
            committer.login AS committer_login,
            transform(parents, parent -> parent.sha) AS parent_shas,
            row_number() OVER (PARTITION BY pull_number, sha) AS row_num
        FROM ${source}.repos_pulls_commits
        WHERE owner = '${owner}' AND repo = '${repo}' AND per_page = 100
            AND pull_number IN (SELECT parent_id FROM batch)
    )
    WHERE row_num = 1
) s
ON t.owner = s.owner AND t.repo = s.repo AND t.pull_number = s.pull_number AND t.sha = s.sha
WHEN NOT MATCHED THEN INSERT (
    owner,
    repo,
    sha,
    pull_number,
    commit_message,
    commit_tree_sha,
    commit_comment_count,
    commit_verified,
    commit_verification_reason,
    author_name,
    author_email,
    author_date,
    author_id,
    author_login,
    committer_name,
    committer_email,
    committer_date,
    committer_id,
    committer_login,
    parent_shas
)
VALUES (
    s.owner,
    s.repo,
    s.sha,
    s.pull_number,
    s.commit_message,
    s.commit_tree_sha,
    s.commit_comment_count,
    s.commit_verified,
    s.commit_verification_reason,
    s.author_name,
    s.author_email,
    s.author_date,
    s.author_id,
    s.author_login,
    s.committer_name,
    s.committer_email,
    s.committer_date,
    s.committer_id,
    s.committer_login,
    s.parent_shas
);

INSERT INTO ${target}.sync_state (step, owner, repo, parent_id, version, checked_at)
WITH parents AS (
    SELECT number AS parent_id, CAST(updated_at AS varchar) AS version, updated_at
    FROM ${target}.pulls
    WHERE owner = '${owner}' AND repo = '${repo}'
),
checked AS (
    SELECT parent_id, max_by(version, checked_at) AS version
    FROM ${target}.sync_state
    WHERE step = 'pull_commits' AND owner = '${owner}' AND repo = '${repo}'
    GROUP BY parent_id
),
batch AS (
    SELECT p.parent_id, p.version
    FROM parents p
    LEFT JOIN checked c ON c.parent_id = p.parent_id
    WHERE c.parent_id IS NULL OR (c.version <> p.version AND p.updated_at > current_timestamp - INTERVAL '3' DAY)
    ORDER BY p.parent_id
    LIMIT ${batch_size:30}
)
SELECT 'pull_commits', '${owner}', '${repo}', parent_id, version, CAST(current_timestamp AS timestamp(6))
FROM batch;
