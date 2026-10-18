-- Jobs of all attempts of completed workflow runs from the last 2 months,
-- read again when a run has a new attempt.
CREATE SCHEMA IF NOT EXISTS ${target};

CREATE TABLE IF NOT EXISTS ${target}.sync_state (
    step varchar,
    owner varchar,
    repo varchar,
    parent_id bigint,
    version varchar,
    checked_at timestamp(6)
);

CREATE TABLE IF NOT EXISTS ${target}.jobs AS
SELECT
    owner,
    repo,
    id,
    run_id,
    run_attempt,
    node_id,
    head_sha,
    head_branch,
    workflow_name,
    status,
    conclusion,
    created_at,
    started_at,
    completed_at,
    name,
    steps,
    labels,
    runner_id,
    runner_name,
    runner_group_name
FROM ${source}.repos_actions_runs_jobs
WHERE owner = '${owner}' AND repo = '${repo}'
WITH NO DATA;

-- loop: batches
MERGE INTO ${target}.jobs t
USING (
    WITH parents AS (
        SELECT id AS parent_id, CAST(max(run_attempt) AS varchar) AS version
        FROM ${target}.runs
        WHERE owner = '${owner}' AND repo = '${repo}' AND created_at > current_timestamp - INTERVAL '2' MONTH
        GROUP BY id
    ),
    checked AS (
        SELECT parent_id, max_by(version, checked_at) AS version
        FROM ${target}.sync_state
        WHERE step = 'jobs' AND owner = '${owner}' AND repo = '${repo}'
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
            run_id,
            run_attempt,
            node_id,
            head_sha,
            head_branch,
            workflow_name,
            status,
            conclusion,
            created_at,
            started_at,
            completed_at,
            name,
            steps,
            labels,
            runner_id,
            runner_name,
            runner_group_name,
            row_number() OVER (PARTITION BY id) AS row_num
        FROM ${source}.repos_actions_runs_jobs
        WHERE owner = '${owner}' AND repo = '${repo}' AND filter = 'all' AND per_page = 100
            AND run_id IN (SELECT parent_id FROM batch)
    )
    WHERE row_num = 1
) s
ON t.id = s.id
WHEN NOT MATCHED THEN INSERT (
    owner,
    repo,
    id,
    run_id,
    run_attempt,
    node_id,
    head_sha,
    head_branch,
    workflow_name,
    status,
    conclusion,
    created_at,
    started_at,
    completed_at,
    name,
    steps,
    labels,
    runner_id,
    runner_name,
    runner_group_name
)
VALUES (
    s.owner,
    s.repo,
    s.id,
    s.run_id,
    s.run_attempt,
    s.node_id,
    s.head_sha,
    s.head_branch,
    s.workflow_name,
    s.status,
    s.conclusion,
    s.created_at,
    s.started_at,
    s.completed_at,
    s.name,
    s.steps,
    s.labels,
    s.runner_id,
    s.runner_name,
    s.runner_group_name
);

INSERT INTO ${target}.sync_state (step, owner, repo, parent_id, version, checked_at)
WITH parents AS (
    SELECT id AS parent_id, CAST(max(run_attempt) AS varchar) AS version
    FROM ${target}.runs
    WHERE owner = '${owner}' AND repo = '${repo}' AND created_at > current_timestamp - INTERVAL '2' MONTH
    GROUP BY id
),
checked AS (
    SELECT parent_id, max_by(version, checked_at) AS version
    FROM ${target}.sync_state
    WHERE step = 'jobs' AND owner = '${owner}' AND repo = '${repo}'
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
SELECT 'jobs', '${owner}', '${repo}', parent_id, version, CAST(current_timestamp AS timestamp(6))
FROM batch;
