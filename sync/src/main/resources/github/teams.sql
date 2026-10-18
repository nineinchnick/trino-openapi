-- Teams of the organization set as the owner. GitHub doesn't always return when a team was created, so the time
-- when the sync first saw it is saved as created_at, and removed_at is set when it's no longer returned.
CREATE SCHEMA IF NOT EXISTS ${target};

CREATE TABLE IF NOT EXISTS ${target}.teams AS
SELECT
    org,
    id,
    node_id,
    url,
    html_url,
    name,
    slug,
    description,
    privacy,
    permission,
    members_url,
    repositories_url,
    parent.id AS parent_id,
    parent.slug AS parent_slug,
    CAST(current_timestamp AS timestamp(6)) AS created_at,
    CAST(current_timestamp AS timestamp(6)) AS removed_at
FROM ${source}.orgs_teams
WHERE org = '${owner}'
WITH NO DATA;

MERGE INTO ${target}.teams t
USING (
    SELECT
        coalesce(src.id, dst.id) AS id,
        src.node_id,
        src.url,
        src.html_url,
        src.name,
        src.slug,
        src.description,
        src.privacy,
        src.permission,
        src.members_url,
        src.repositories_url,
        src.parent_id,
        src.parent_slug,
        src.id IS NULL AS removed
    FROM (
        SELECT DISTINCT
            id,
            node_id,
            url,
            html_url,
            name,
            slug,
            description,
            privacy,
            permission,
            members_url,
            repositories_url,
            parent.id AS parent_id,
            parent.slug AS parent_slug
        FROM ${source}.orgs_teams
        WHERE org = '${owner}' AND per_page = 100
    ) src
    FULL JOIN (
        SELECT id
        FROM ${target}.teams
        WHERE org = '${owner}' AND removed_at IS NULL
    ) dst ON dst.id = src.id
) s
ON t.org = '${owner}' AND t.id = s.id AND t.removed_at IS NULL
WHEN MATCHED AND s.removed THEN UPDATE SET
    removed_at = CAST(current_timestamp AS timestamp(6))
WHEN MATCHED AND (
    s.name IS DISTINCT FROM t.name
    OR s.slug IS DISTINCT FROM t.slug
    OR s.description IS DISTINCT FROM t.description
    OR s.privacy IS DISTINCT FROM t.privacy
    OR s.permission IS DISTINCT FROM t.permission
    OR s.parent_id IS DISTINCT FROM t.parent_id) THEN UPDATE SET
    name = s.name,
    slug = s.slug,
    description = s.description,
    privacy = s.privacy,
    permission = s.permission,
    parent_id = s.parent_id,
    parent_slug = s.parent_slug
WHEN NOT MATCHED THEN INSERT (
    org,
    id,
    node_id,
    url,
    html_url,
    name,
    slug,
    description,
    privacy,
    permission,
    members_url,
    repositories_url,
    parent_id,
    parent_slug,
    created_at,
    removed_at
)
VALUES (
    '${owner}',
    s.id,
    s.node_id,
    s.url,
    s.html_url,
    s.name,
    s.slug,
    s.description,
    s.privacy,
    s.permission,
    s.members_url,
    s.repositories_url,
    s.parent_id,
    s.parent_slug,
    CAST(current_timestamp AS timestamp(6)),
    NULL
);
