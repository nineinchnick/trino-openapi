-- Members of the organization set as the owner, with an empty team_slug, and members of its teams.
-- GitHub doesn't return when someone became a member, so the time when the sync first saw them is saved
-- as joined_at, and removed_at is set when they're no longer returned. A member who comes back gets a new row.
CREATE SCHEMA IF NOT EXISTS ${target};

CREATE TABLE IF NOT EXISTS ${target}.members AS
SELECT
    org,
    CAST('' AS varchar) AS team_slug,
    login,
    id,
    type,
    site_admin,
    CAST(current_timestamp AS timestamp(6)) AS joined_at,
    CAST(current_timestamp AS timestamp(6)) AS removed_at
FROM ${source}.orgs_members
WHERE org = '${owner}'
WITH NO DATA;

MERGE INTO ${target}.members t
USING (
    SELECT
        coalesce(src.team_slug, dst.team_slug) AS team_slug,
        coalesce(src.id, dst.id) AS id,
        src.login,
        src.type,
        src.site_admin,
        src.id IS NULL AS removed
    FROM (
        SELECT '' AS team_slug, login, id, type, site_admin
        FROM ${source}.orgs_members
        WHERE org = '${owner}' AND per_page = 100
        UNION
        SELECT team_slug, login, id, type, site_admin
        FROM ${source}.orgs_teams_members
        WHERE org = '${owner}' AND per_page = 100
            AND team_slug IN (SELECT slug FROM ${source}.orgs_teams WHERE org = '${owner}' AND per_page = 100)
    ) src
    FULL JOIN (
        SELECT team_slug, id
        FROM ${target}.members
        WHERE org = '${owner}' AND removed_at IS NULL
    ) dst ON dst.team_slug = src.team_slug AND dst.id = src.id
) s
ON t.org = '${owner}' AND t.team_slug = s.team_slug AND t.id = s.id AND t.removed_at IS NULL
WHEN MATCHED AND s.removed THEN UPDATE SET
    removed_at = CAST(current_timestamp AS timestamp(6))
WHEN NOT MATCHED THEN INSERT (
    org,
    team_slug,
    login,
    id,
    type,
    site_admin,
    joined_at,
    removed_at
)
VALUES (
    '${owner}',
    s.team_slug,
    s.login,
    s.id,
    s.type,
    s.site_admin,
    CAST(current_timestamp AS timestamp(6)),
    NULL
);
