-- Completed workflow runs, so they don't have to be updated later. Every attempt is saved separately.
-- The endpoint has no parameter to read only recent runs, and returns the newest first,
-- so read pages until two pages in a row have no new runs, because runs don't complete in the order they started.
CREATE SCHEMA IF NOT EXISTS ${target};

CREATE TABLE IF NOT EXISTS ${target}.runs AS
SELECT
    owner,
    repo,
    id,
    name,
    node_id,
    check_suite_id,
    check_suite_node_id,
    head_branch,
    head_sha,
    path,
    run_number,
    run_attempt,
    event,
    status,
    conclusion,
    workflow_id,
    display_title,
    actor.id AS actor_id,
    actor.login AS actor_login,
    triggering_actor.login AS triggering_actor_login,
    transform(pull_requests, pull -> pull.number) AS pull_request_numbers,
    created_at,
    updated_at,
    run_started_at
FROM ${source}.repos_actions_runs
WHERE owner = '${owner}' AND repo = '${repo}'
WITH NO DATA;

-- loop: pages
-- stop-after: 2
MERGE INTO ${target}.runs t
USING (
    SELECT *
    FROM (
        SELECT
            owner,
            repo,
            id,
            name,
            node_id,
            check_suite_id,
            check_suite_node_id,
            head_branch,
            head_sha,
            path,
            run_number,
            run_attempt,
            event,
            status,
            conclusion,
            workflow_id,
            display_title,
            actor.id AS actor_id,
            actor.login AS actor_login,
            triggering_actor.login AS triggering_actor_login,
            transform(pull_requests, pull -> pull.number) AS pull_request_numbers,
            created_at,
            updated_at,
            run_started_at,
            row_number() OVER (PARTITION BY id, run_attempt) AS row_num
        FROM ${source}.repos_actions_runs
        WHERE owner = '${owner}' AND repo = '${repo}' AND status = 'completed' AND per_page = 100 AND page = ${page}
    )
    WHERE row_num = 1
) s
ON t.id = s.id AND t.run_attempt = s.run_attempt
WHEN NOT MATCHED THEN INSERT (
    owner,
    repo,
    id,
    name,
    node_id,
    check_suite_id,
    check_suite_node_id,
    head_branch,
    head_sha,
    path,
    run_number,
    run_attempt,
    event,
    status,
    conclusion,
    workflow_id,
    display_title,
    actor_id,
    actor_login,
    triggering_actor_login,
    pull_request_numbers,
    created_at,
    updated_at,
    run_started_at
)
VALUES (
    s.owner,
    s.repo,
    s.id,
    s.name,
    s.node_id,
    s.check_suite_id,
    s.check_suite_node_id,
    s.head_branch,
    s.head_sha,
    s.path,
    s.run_number,
    s.run_attempt,
    s.event,
    s.status,
    s.conclusion,
    s.workflow_id,
    s.display_title,
    s.actor_id,
    s.actor_login,
    s.triggering_actor_login,
    s.pull_request_numbers,
    s.created_at,
    s.updated_at,
    s.run_started_at
);
