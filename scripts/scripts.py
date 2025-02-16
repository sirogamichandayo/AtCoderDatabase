import argparse
import os
import time
import math
import datetime
import clickhouse_connect
import requests


def request_with_wait(method, url, **kwargs):
    response = requests.request(method, url, **kwargs)
    time.sleep(2)
    return response

def make_clickhouse_client():
    dsn = os.getenv("CLICKHOUSE_DSN")
    client = clickhouse_connect.get_client(dsn=dsn)
    return client

def complete_submissions(args):
    client = make_clickhouse_client()
    client.command(f"OPTIMIZE TABLE atcoder.submissions FINAL")

def update_rating_history(args):
    """
    コンテスト一覧を取得して、未更新のレーティングを insert
    """
    client = make_clickhouse_client()

    # 有志 api からコンテスト一覧を取得
    url = "https://kenkoooo.com/atcoder/resources/contests.json"
    headers = {"Accept-Encoding": "gzip"}
    response = request_with_wait('GET', url, headers=headers)
    response.raise_for_status()
    contests = response.json()

    updated_contest_map = {}
    for contest in contests:
        contest_id = contest.get("id")
        updated_contest_map[contest_id] = False

    # すでに存在するコンテスト screen_name を取得, ex : ahc001.contest.atcoder.jp
    query = "SELECT DISTINCT contest_screen_name FROM atcoder.rating_history"
    updated_names = {row[0] for row in client.query(query).result_rows}
    for name in updated_names:
        # 多分これで contest id が取れるはず
        contest_id = name.split(".")[0]
        updated_contest_map[contest_id] = True

    for contest_id, is_updated in updated_contest_map.items():
        if is_updated:
            continue

        url = f"https://atcoder.jp/contests/{contest_id}/results/json"
        response = request_with_wait('GET', url)
        response.raise_for_status()
        data = response.json()

        rows_to_insert = []
        for result in data:
            row = (
                result.get("UserScreenName"),  # = user_id
                contest_id,
                result.get("IsRated"),
                result.get("Place"),
                result.get("OldRating"),
                result.get("NewRating"),
                result.get("Performance"),
                result.get("ContestName"),
                result.get("ContestNameEn"),
                result.get("ContestScreenName"),
                datetime.datetime.fromisoformat(result.get("EndTime")),
                result.get("ContestType"),
                result.get("UserName"),
                result.get("Country"),
                result.get("Affiliation"),
                result.get("Rating"),
                result.get("Competitions"),
                result.get("AtCoderRank")
            )
            rows_to_insert.append(row)

        client.insert(
            table="atcoder.rating_history",
            data=rows_to_insert,
            column_names=[
                "user_id",
                "contest_id",
                "is_rated",
                "place",
                "old_rating",
                "new_rating",
                "performance",
                "contest_name",
                "contest_name_en",
                "contest_screen_name",
                "end_time",
                "contest_type",
                "user_name",
                "country",
                "affiliation",
                "rating",
                "competitions",
                "atcoder_rank"
            ]
        )



def update_problem_models(args):
    client = make_clickhouse_client()

    tmp_table_name = f"problem_models_{time.strftime('%Y%m%d_%H%M%S', time.localtime())}"
    original_table_name = "problem_models"
    client.command(f"""
CREATE TABLE atcoder.{tmp_table_name}
(
    `problem_id`       String,
    `slope`            Nullable(Float64),
    `intercept`        Nullable(Float64),
    `variance`         Nullable(Float64),
    `difficulty`       Nullable(Int32),
    `clip_difficulty`  Nullable(Int32),
    `discrimination`   Nullable(Float64),
    `irt_loglikelihood` Nullable(Float64),
    `irt_users`        Nullable(UInt32),
    `is_experimental`  Nullable(Bool),
    `contest_id`       String,
    `problem_index`    String,
    `name`             String,
    `title`            String,
    `contest_type`     Enum8('algorithm' = 0, 'heuristic' = 1)
)
ENGINE = MergeTree
ORDER BY problem_id
SETTINGS index_granularity = 8192
""")

    url = "https://kenkoooo.com/atcoder/resources/problems.json"
    headers = {"Accept-Encoding": "gzip"}
    response = request_with_wait('GET', url, headers=headers)
    response.raise_for_status()
    problems = response.json()

    url = "https://kenkoooo.com/atcoder/resources/problem-models.json"
    headers = {"Accept-Encoding": "gzip"}
    response = request_with_wait('GET', url, headers=headers)
    response.raise_for_status()
    problem_models = response.json()

    rows_to_insert = []
    contest_type_map = {
        # "contest_id": "contest type"
    }

    for problem in problems:
        problem_id = problem.get("id")
        model = problem_models.get(problem_id) or {}
        difficulty = model.get("difficulty")
        if difficulty is not None:
            # see: https://github.com/kenkoooo/AtCoderProblems/blob/master/lambda-functions/time-estimator/rating.py#L30-L33
            clip_difficulty = round(
                difficulty if difficulty >= 400 else 400 / math.exp(1.0 - difficulty / 400)
            )
        else:
            clip_difficulty = None


        # get contest type
        contest_id = problem.get("contest_id")
        if contest_id not in contest_type_map:
            url = f"https://atcoder.jp/api/contests/{contest_id}"
            response = request_with_wait('GET', url)
            response.raise_for_status()
            problem_models = response.json()
            contest_type = problem_models.get("ContestType")
            contest_type_map[contest_id] = contest_type

        row = (
            problem_id,
            model.get("slope"),
            model.get("intercept"),
            model.get("variance"),
            model.get("difficulty"),
            clip_difficulty,
            model.get("discrimination"),
            model.get("irt_loglikelihood"),
            model.get("irt_users"),
            model.get("is_experimental"),
            problem.get("contest_id"),
            problem.get("problem_index"),
            problem.get("name"),
            problem.get("title"),
            contest_type_map[contest_id]
        )

        rows_to_insert.append(row)

    client.insert(
        table=tmp_table_name,
        data=rows_to_insert,
        column_names=[
            "problem_id",
            "slope",
            "intercept",
            "variance",
            "difficulty",
            "clip_difficulty",
            "discrimination",
            "irt_loglikelihood",
            "irt_users",
            "is_experimental",
            "contest_id",
            "problem_index",
            "name",
            "title",
            "contest_type"
        ]
    )

    client.command(f"EXCHANGE TABLES atcoder.{original_table_name} AND atcoder.{tmp_table_name}")

    client.command(f"DROP TABLE atcoder.{tmp_table_name}")

def update_submissions(args):
    """
    1. ClickHouse から最初の unixtime を取得
    2. unixtime を使って API を叩く
    3. 取得データを ClickHouse に upsert
    4. 取得配列が空なら次の処理を停止
    5. 一番最後の unixtime で再度 API を叩く
    """

    client = make_clickhouse_client()

    while True:
        # (2) current_unix_time を使って API を叩く (with compressed request handling)
        result = client.query(f"SELECT max(epoch_second) FROM atcoder.submissions")
        current_unix_time = result.result_rows[0][0]
        print(f"Requesting with unixtime: {current_unix_time}")
        url = f"https://kenkoooo.com/atcoder/atcoder-api/v3/from/{current_unix_time+1}"
        headers = {"Accept-Encoding": "gzip"}
        response = request_with_wait('GET', url, headers=headers)
        response.raise_for_status()

        # 取得したデータを JSON としてパース
        data_list = response.json()
        if len(data_list) == 0:
            break

        # (3) ClickHouse への upsert 処理
        rows_to_insert = []
        for item in data_list:
            row = (
                item.get("id"),
                item.get("epoch_second"),
                item.get("problem_id"),
                item.get("contest_id"),
                item.get("user_id"),
                item.get("language"),
                item.get("point"),
                item.get("length"),
                item.get("result"),
                item.get("execution_time"),
            )
            rows_to_insert.append(row)

        # Client.insert の使用例
        # 'atcoder.replaceable_submissions' テーブルのカラム名を指定して、データの挿入を実行します。
        client.insert(
            table="atcoder.submissions",
            data=rows_to_insert,
            column_names=[
                "id",
                "epoch_second",
                "problem_id",
                "contest_id",
                "user_id",
                "language",
                "point",
                "length",
                "result",
                "execution_time"
            ]
        )

def all_update_submissions(args):
    """
    1. ClickHouse から最初の unixtime を取得
    2. unixtime を使って API を叩く
    3. 取得データを ClickHouse に upsert
    4. 取得配列が空なら次の処理を停止
    5. 一番最後の unixtime で再度 API を叩く
    """
    # TODO: 全く使ってない実装、全部更新するような実装に直す

    client = make_clickhouse_client()

    current_unixtime = 0

    while True:
        print(f"Requesting with unixtime: {current_unixtime}")
        url = f"https://kenkoooo.com/atcoder/atcoder-api/v3/from/{current_unixtime}"
        headers = {"Accept-Encoding": "gzip"}
        response = request_with_wait('GET', url, headers=headers)
        response.raise_for_status()

        # 取得したデータを JSON としてパース
        data_list = response.json()

        # (3) ClickHouse への upsert 処理
        rows_to_insert = []
        for item in data_list:
            row = (
                item.get("id"),
                item.get("epoch_second"),
                item.get("problem_id"),
                item.get("contest_id"),
                item.get("user_id"),
                item.get("language"),
                item.get("point"),
                item.get("length"),
                item.get("result"),
                item.get("execution_time"),
            )
            rows_to_insert.append(row)

        client.insert(
            table="atcoder.replaceable_submissions",
            data=rows_to_insert,
            column_names=[
                "id",
                "epoch_second",
                "problem_id",
                "contest_id",
                "user_id",
                "language",
                "point",
                "length",
                "result",
                "execution_time"
            ]
        )

        if len(data_list) == 0:
            break

        last_item = data_list[-1]
        if "epoch_second" in last_item:
            current_unixtime = last_item["epoch_second"] + 1

        time.sleep(2)

def main():
    parser = argparse.ArgumentParser(
        description="シンプルなコマンドラインツール (fetch/insert)"
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="実行するサブコマンド")

    # insert コマンド
    parser_insert = subparsers.add_parser("insert", help="ClickHouseにデータを挿入する")
    parser_insert.set_defaults(func=update_submissions)
    # 必要に応じて insert 用のオプションを追加可能

    parser_insert = subparsers.add_parser("insert_all", help="ClickHouseにデータを挿入する")
    parser_insert.set_defaults(func=all_update_submissions)

    parser_insert = subparsers.add_parser("complete", help="submissionsテーブルに Complete 処理を走らせる")
    parser_insert.set_defaults(func=complete_submissions)

    parser_insert = subparsers.add_parser("update_problems", help="problem modelテーブルを最新に更新する")
    parser_insert.set_defaults(func=update_problem_models)

    parser_insert = subparsers.add_parser("update_rating", help="レーティングテーブル更新")
    parser_insert.set_defaults(func=update_rating_history)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
