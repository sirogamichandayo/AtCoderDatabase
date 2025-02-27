-- このテーブルは毎回入れ替えるからダミーのテーブルで OK
CREATE TABLE atcoder.problem_models
(
    problem_id String,
)
ENGINE = MergeTree;

CREATE TABLE atcoder.submissions (
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
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (user_id, epoch_second, id)
SETTINGS index_granularity = 8192;


CREATE TABLE atcoder.rating_history (
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
) ENGINE = ReplacingMergeTree()
ORDER BY (user_id, end_time, contest_id)
SETTINGS index_granularity = 8192;
