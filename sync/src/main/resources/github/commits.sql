-- Commits of the default branch. The cursor is sent as the since parameter, which GitHub compares
-- with the commit date, so the newest commit is read again, and the MERGE skips it.
CREATE SCHEMA IF NOT EXISTS ${target};

CREATE TABLE IF NOT EXISTS ${target}.commits AS
SELECT
    owner,
    repo,
    sha,
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
FROM ${source}.repos_commits
WHERE owner = '${owner}' AND repo = '${repo}'
WITH NO DATA;

MERGE INTO ${target}.commits t
USING (
    SELECT *
    FROM (
        SELECT
            owner,
            repo,
            sha,
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
            row_number() OVER (PARTITION BY sha) AS row_num
        FROM ${source}.repos_commits
        WHERE owner = '${owner}' AND repo = '${repo}' AND per_page = 100
            AND since = (SELECT CAST(coalesce(max(committer_date), TIMESTAMP '2000-01-01') AS timestamp(3)) FROM ${target}.commits WHERE owner = '${owner}' AND repo = '${repo}')
    )
    WHERE row_num = 1
) s
ON t.owner = s.owner AND t.repo = s.repo AND t.sha = s.sha
WHEN NOT MATCHED THEN INSERT (
    owner,
    repo,
    sha,
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
