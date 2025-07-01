CREATE TABLE atcoder.problem_models ON CLUSTER cluster1
(
    `problem_id`       String,
    `slope`            Float64 DEFAULT nan,
    `intercept`        Float64 DEFAULT nan,
    `variance`         Float64 DEFAULT nan,
    `difficulty`       Int32 DEFAULT -123456789,
    `clip_difficulty`  Int32 DEFAULT -123456789,
    `discrimination`   Float64 DEFAULT nan,
    `irt_loglikelihood` Float64 DEFAULT nan,
    `irt_users`        Int32 DEFAULT -1,
    `is_experimental`  Int8 DEFAULT -1,
    `contest_id`       String,
    `problem_index`    String,
    `name`             String,
    `title`            String,
    `problem_type`     Enum8('algorithm' = 0, 'heuristic' = 1),
    `contest_type`     Enum8('algorithm' = 0, 'heuristic' = 1),
    `created_at`       DateTime DEFAULT now()
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/cluster1/atcoder.problem_models_new', '{replica}', created_at)
ORDER BY (problem_id, clip_difficulty)
SETTINGS index_granularity = 8192;

CREATE VIEW atcoder.problem_models_view ON CLUSTER cluster1 AS
SELECT
    problem_id,
    argMax(slope, created_at)             AS slope,
    argMax(intercept, created_at)         AS intercept,
    argMax(variance, created_at)          AS variance,
    argMax(difficulty, created_at)        AS difficulty,
    argMax(clip_difficulty, created_at)   AS clip_difficulty,
    argMax(discrimination, created_at)    AS discrimination,
    argMax(irt_loglikelihood, created_at) AS irt_loglikelihood,
    argMax(irt_users, created_at)         AS irt_users,
    argMax(is_experimental, created_at)   AS is_experimental,
    argMax(contest_id, created_at)        AS contest_id,
    argMax(problem_index, created_at)     AS problem_index,
    argMax(name, created_at)              AS name,
    argMax(title, created_at)             AS title,
    argMax(problem_type, created_at)      AS problem_type,
    argMax(contest_type, created_at)      AS contest_type,
    max(created_at)                       AS created_at
FROM atcoder.problem_models
GROUP BY problem_id;

-- submissions テーブル（ReplacingMergeTree 使用）の例
CREATE TABLE atcoder.submissions ON CLUSTER cluster1
(
    id UInt64,
    epoch_second Int64,
    problem_id String,
    contest_id String,
    user_id String,
    language String,
    point Float64,
    length Int32,
    result String,
    execution_time Nullable(Int32), -- TODO: nullable 外す
    created_at DateTime DEFAULT now()
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/cluster1/atcoder.submissions', '{replica}', created_at)
ORDER BY (user_id, epoch_second, id)
SETTINGS index_granularity = 8192;

-- rating_history テーブル（ReplacingMergeTree 使用）の例
CREATE TABLE atcoder.rating_history ON CLUSTER cluster1
(
    user_id String,
    contest_id String,
    is_rated Int8,
    place Int32,
    old_rating Int32,
    new_rating Int32,
    performance Int32,
    contest_name String,
    contest_name_en String,
    contest_screen_name String,
    end_time DateTime,
    contest_type Enum8('algorithm' = 0, 'heuristic' = 1),
    user_name String,
    country String,
    affiliation String,
    rating Int32,
    competitions Int32,
    atcoder_rank Int32
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/cluster1/atcoder.rating_history', '{replica}')
ORDER BY (user_id, end_time, contest_id)
SETTINGS index_granularity = 8192;

CREATE VIEW atcoder.rating_history_rated_latest_view ON CLUSTER cluster1 AS
SELECT
    user_id,
    contest_type,
    argMax(contest_id, end_time) AS contest_id,
    argMax(place, end_time) AS place,
    argMax(old_rating, end_time) AS old_rating,
    argMax(new_rating, end_time) AS new_rating,
    argMax(performance, end_time) AS performance,
    argMax(contest_name, end_time) AS contest_name,
    argMax(contest_name_en, end_time) AS contest_name_en,
    argMax(contest_screen_name, end_time) AS contest_screen_name,
    argMax(end_time, end_time) AS latest_end_time,
    argMax(user_name, end_time) AS user_name,
    argMax(country, end_time) AS country,
    argMax(affiliation, end_time) AS affiliation,
    argMax(rating, end_time) AS rating,
    argMax(competitions, end_time) AS competitions,
    argMax(atcoder_rank, end_time) AS atcoder_rank
FROM atcoder.rating_history
WHERE is_rated=true
GROUP BY
    user_id,
    contest_type;

-- クラスタ上でのレプリケーション対応テーブル作成
CREATE TABLE atcoder.contests ON CLUSTER cluster1
(
    id                 String,
    start_epoch_second DateTime,
    duration_second    Int64,
    title              String,
    rate_change        String,
    created_at         DateTime DEFAULT now()
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/cluster1/atcoder.contests', '{replica}', created_at)
ORDER BY (id, start_epoch_second);

-- クラスタ上でのレプリケーション対応ビュー作成
CREATE VIEW atcoder.contest_view ON CLUSTER cluster1 AS
SELECT
    id,
    argMax(start_epoch_second, created_at) AS start_epoch_second,
    argMax(duration_second, created_at) AS duration_second,
    argMax(title, created_at) AS title,
    argMax(rate_change, created_at) AS rate_change,
    max(created_at) AS last_created_at
FROM atcoder.contests
GROUP BY id;
e
