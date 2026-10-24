-- Comments of all issues and pull requests, except review comments.
CREATE SCHEMA IF NOT EXISTS ${target};

CREATE TABLE IF NOT EXISTS ${target}.issue_comments AS
SELECT
    owner,
    repo,
    id,
    node_id,
    url,
    html_url,
    body,
    user.id AS user_id,
    user.login AS user_login,
    created_at,
    updated_at,
    issue_url,
    CAST(regexp_extract(issue_url, '/issues/(\d+)$', 1) AS bigint) AS issue_number,
    author_association,
    reactions.total_count AS reactions_total_count,
    performed_via_github_app.id AS app_id,
    performed_via_github_app.slug AS app_slug
FROM ${source}.repos_owner_repo_issues_comments_comment_id
WHERE owner = '${owner}' AND repo = '${repo}'
WITH NO DATA;

MERGE INTO ${target}.issue_comments t
USING (
    SELECT *
    FROM (
        SELECT
            owner,
            repo,
            id,
            node_id,
            url,
            html_url,
            body,
            user.id AS user_id,
            user.login AS user_login,
            created_at,
            updated_at,
            issue_url,
            CAST(regexp_extract(issue_url, '/issues/(\d+)$', 1) AS bigint) AS issue_number,
            author_association,
            reactions.total_count AS reactions_total_count,
            performed_via_github_app.id AS app_id,
            performed_via_github_app.slug AS app_slug,
            row_number() OVER (PARTITION BY id ORDER BY updated_at DESC) AS row_num
        FROM ${source}.repos_owner_repo_issues_comments_comment_id
        WHERE owner = '${owner}' AND repo = '${repo}' AND per_page = 100
            AND updated_at >= (SELECT CAST(coalesce(max(updated_at), TIMESTAMP '2000-01-01') AS timestamp(3)) FROM ${target}.issue_comments WHERE owner = '${owner}' AND repo = '${repo}')
    )
    WHERE row_num = 1
) s
ON t.id = s.id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET
    owner = s.owner,
    repo = s.repo,
    id = s.id,
    node_id = s.node_id,
    url = s.url,
    html_url = s.html_url,
    body = s.body,
    user_id = s.user_id,
    user_login = s.user_login,
    created_at = s.created_at,
    updated_at = s.updated_at,
    issue_url = s.issue_url,
    issue_number = s.issue_number,
    author_association = s.author_association,
    reactions_total_count = s.reactions_total_count,
    app_id = s.app_id,
    app_slug = s.app_slug
WHEN NOT MATCHED THEN INSERT (
    owner,
    repo,
    id,
    node_id,
    url,
    html_url,
    body,
    user_id,
    user_login,
    created_at,
    updated_at,
    issue_url,
    issue_number,
    author_association,
    reactions_total_count,
    app_id,
    app_slug
)
VALUES (
    s.owner,
    s.repo,
    s.id,
    s.node_id,
    s.url,
    s.html_url,
    s.body,
    s.user_id,
    s.user_login,
    s.created_at,
    s.updated_at,
    s.issue_url,
    s.issue_number,
    s.author_association,
    s.reactions_total_count,
    s.app_id,
    s.app_slug
);
