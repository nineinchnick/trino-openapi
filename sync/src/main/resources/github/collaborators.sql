-- Collaborators of the repository. GitHub doesn't return when someone became a collaborator, so the time when
-- the sync first saw them is saved as joined_at, and removed_at is set when they're no longer returned.
-- A collaborator who comes back gets a new row.
CREATE SCHEMA IF NOT EXISTS ${target};

CREATE TABLE IF NOT EXISTS ${target}.collaborators AS
SELECT
    owner,
    repo,
    login,
    id,
    type,
    site_admin,
    permissions.pull AS permission_pull,
    permissions.triage AS permission_triage,
    permissions.push AS permission_push,
    permissions.maintain AS permission_maintain,
    permissions.admin AS permission_admin,
    role_name,
    CAST(current_timestamp AS timestamp(6)) AS joined_at,
    CAST(current_timestamp AS timestamp(6)) AS removed_at
FROM ${source}.repos_collaborators
WHERE owner = '${owner}' AND repo = '${repo}'
WITH NO DATA;

MERGE INTO ${target}.collaborators t
USING (
    SELECT
        coalesce(src.login, dst.login) AS login,
        src.id,
        src.type,
        src.site_admin,
        src.permission_pull,
        src.permission_triage,
        src.permission_push,
        src.permission_maintain,
        src.permission_admin,
        src.role_name,
        src.login IS NULL AS removed
    FROM (
        SELECT DISTINCT
            login,
            id,
            type,
            site_admin,
            permissions.pull AS permission_pull,
            permissions.triage AS permission_triage,
            permissions.push AS permission_push,
            permissions.maintain AS permission_maintain,
            permissions.admin AS permission_admin,
            role_name
        FROM ${source}.repos_collaborators
        WHERE owner = '${owner}' AND repo = '${repo}' AND per_page = 100
    ) src
    FULL JOIN (
        SELECT login
        FROM ${target}.collaborators
        WHERE owner = '${owner}' AND repo = '${repo}' AND removed_at IS NULL
    ) dst ON dst.login = src.login
) s
ON t.owner = '${owner}' AND t.repo = '${repo}' AND t.login = s.login AND t.removed_at IS NULL
WHEN MATCHED AND s.removed THEN UPDATE SET
    removed_at = CAST(current_timestamp AS timestamp(6))
WHEN MATCHED AND (
    s.permission_pull IS DISTINCT FROM t.permission_pull
    OR s.permission_triage IS DISTINCT FROM t.permission_triage
    OR s.permission_push IS DISTINCT FROM t.permission_push
    OR s.permission_maintain IS DISTINCT FROM t.permission_maintain
    OR s.permission_admin IS DISTINCT FROM t.permission_admin
    OR s.role_name IS DISTINCT FROM t.role_name) THEN UPDATE SET
    permission_pull = s.permission_pull,
    permission_triage = s.permission_triage,
    permission_push = s.permission_push,
    permission_maintain = s.permission_maintain,
    permission_admin = s.permission_admin,
    role_name = s.role_name
WHEN NOT MATCHED THEN INSERT (
    owner,
    repo,
    login,
    id,
    type,
    site_admin,
    permission_pull,
    permission_triage,
    permission_push,
    permission_maintain,
    permission_admin,
    role_name,
    joined_at,
    removed_at
)
VALUES (
    '${owner}',
    '${repo}',
    s.login,
    s.id,
    s.type,
    s.site_admin,
    s.permission_pull,
    s.permission_triage,
    s.permission_push,
    s.permission_maintain,
    s.permission_admin,
    s.role_name,
    CAST(current_timestamp AS timestamp(6)),
    NULL
);
