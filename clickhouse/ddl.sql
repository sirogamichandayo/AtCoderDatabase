-- このテーブルは毎回入れ替えるからダミーのテーブルで OK
CREATE TABLE atcoder.problem_models
(
    problem_id String,
)
ENGINE = MergeTree;

CREATE TABLE atcoder.submissions (
    id UInt64,
    epoch_second UInt64,
    problem_id String,
    contest_id String,
    user_id String,
    language String,
    point Float64,
    length UInt32,
    result String,
    execution_time UInt32 DEFAULT -1
) ENGINE = ReplacingMergeTree(id)
ORDER BY epoch_second
SETTINGS index_granularity = 8192;

CREATE TABLE atcoder.rating_history (
    user_id String,
    contest_id String,
    is_rated UInt8,
    place UInt32,
    old_rating Int32,
    new_rating Int32,
    performance Int32,
    contest_name String,
    contest_name_en String,
    contest_screen_name String,
    end_time DateTime,
    contest_type String,
    user_name String,
    country String,
    affiliation String,
    rating Int32,
    competitions UInt32,
    atcoder_rank UInt32
) ENGINE = ReplacingMergeTree()
ORDER BY (user_id, end_time, contest_id)
SETTINGS index_granularity = 8192;
