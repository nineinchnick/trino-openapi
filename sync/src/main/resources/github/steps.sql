-- Steps of jobs from the last 2 months, which are included in the jobs, so this doesn't make any requests.
CREATE SCHEMA IF NOT EXISTS ${target};

CREATE TABLE IF NOT EXISTS ${target}.steps AS
SELECT
    j.owner,
    j.repo,
    j.run_id,
    j.run_attempt,
    j.id AS job_id,
    s.number,
    s.name,
    s.status,
    s.conclusion,
    s.started_at,
    s.completed_at
FROM ${target}.jobs j
CROSS JOIN UNNEST(j.steps) AS s (status, conclusion, name, number, started_at, completed_at)
WITH NO DATA;

MERGE INTO ${target}.steps t
USING (
    SELECT
        j.owner,
        j.repo,
        j.run_id,
        j.run_attempt,
        j.id AS job_id,
        s.number,
        s.name,
        s.status,
        s.conclusion,
        s.started_at,
        s.completed_at
    FROM ${target}.jobs j
    CROSS JOIN UNNEST(j.steps) AS s (status, conclusion, name, number, started_at, completed_at)
    WHERE j.owner = '${owner}' AND j.repo = '${repo}' AND j.started_at > current_timestamp - INTERVAL '2' MONTH
) s
ON t.job_id = s.job_id AND t.number = s.number
WHEN NOT MATCHED THEN INSERT (
    owner,
    repo,
    run_id,
    run_attempt,
    job_id,
    number,
    name,
    status,
    conclusion,
    started_at,
    completed_at
)
VALUES (
    s.owner,
    s.repo,
    s.run_id,
    s.run_attempt,
    s.job_id,
    s.number,
    s.name,
    s.status,
    s.conclusion,
    s.started_at,
    s.completed_at
);
