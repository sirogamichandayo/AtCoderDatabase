import argparse
import os
import time
import math
import datetime
import clickhouse_connect
import requests

def none_get(value, default):
    return value if value is not None else default

def request_with_wait(method, url, **kwargs):
    response = requests.request(method, url, **kwargs)
    time.sleep(2)
    return response

def make_clickhouse_client():
    dsn = os.getenv("CLICKHOUSE_DSN")
    client = clickhouse_connect.get_client(dsn=dsn)
    return client

def complete_tables(args):
    client = make_clickhouse_client()
    client.command(f"OPTIMIZE TABLE atcoder.submissions on cluster cluster1 FINAL")
    client.command(f"OPTIMIZE TABLE atcoder.rating_history on cluster cluster1 FINAL")
    client.command(f"OPTIMIZE TABLE atcoder.problem_models on cluster cluster1 FINAL")

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

        print(f"start {contest_id}")
        url = f"https://atcoder.jp/contests/{contest_id}/results/json"
        response = request_with_wait('GET', url)
        response.raise_for_status()
        data = response.json()

        if len(data) == 0:
            continue

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
            data=rows_to_insert
        )
        print(f"success {contest_id}")

def insert_contest_models(args):
    client = make_clickhouse_client()

    # JSON データを取得する URL（例として既知の JSON を使う場合）
    url = "https://kenkoooo.com/atcoder/resources/contests.json"
    headers = {"Accept-Encoding": "gzip"}
    response = request_with_wait('GET', url, headers=headers)
    response.raise_for_status()
    contests = response.json()  # JSON はリスト形式になっている想定

    rows_to_insert = []
    for contest in contests:
        # テーブル atcoder.contests のカラムは
        # (id, start_epoch_second, duration_second, title, rate_change, created_at)
        # created_at は DEFAULT now() で自動設定されるため、ここでは挿入対象外
        row = (
            contest.get("id"),
            contest.get("start_epoch_second"),
            contest.get("duration_second"),
            contest.get("title"),
            contest.get("rate_change")
        )
        rows_to_insert.append(row)

    client.insert(
        table="atcoder.contests",
        data=rows_to_insert,
        column_names=["id", "start_epoch_second", "duration_second", "title", "rate_change"]
    )


def update_problem_models(args):
    client = make_clickhouse_client()

    default_values = {
        "slope": float("nan"),
        "intercept": float("nan"),
        "variance": float("nan"),
        "difficulty": -123456789,
        "clip_difficulty": -123456789,
        "discrimination": float("nan"),
        "irt_loglikelihood": float("nan"),
        "irt_users": -1,
        "is_experimental": -1,
    }

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
    # TODO: API 叩く数を減らすために既存データからデータを埋める、contest type は何もいじってないのでできるはず
    contest_type_map = {
        # "contest_id": "contest type"
    }

    for problem in problems:
        problem_id = problem.get("id")
        model = problem_models.get(problem_id) or {}
        print(f"problem_id: {problem_id}, model: {model}")
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
            url = f"https://kenkoooo.com/atcoder/resources/problems.json"
            response = request_with_wait('GET', url)
            response.raise_for_status()
            p = response.json()
            contest_type = p.get("ContestType")
            contest_type_map[contest_id] = contest_type

        contest_type = contest_type_map[contest_id]
        problem_type = contest_type_map[contest_id]
        # 一部ヒュのがアルゴとして検出されるので弾く、コンテストとしては "algorithm" だけど問題としては "heuristic" になるものの一覧
        # 絶対ミスあるけどしゃーなし
        heuristic_list = [
            # 2718 点よりでかい点数
            "tessoku_book_fr",
            "tessoku_book_at",
            "tessoku_book_aw",
            "joisc2017_e",
            "ddcc2019_machine_a",
            "math_and_algorithm_bw",
            "pakencamp_2019_day2_a",
            "math_and_algorithm_bx",
            "ddcc2019_machine_b",

            # 提出されたポイントの種類が30以上のもの、絶対ミスある
            "s8pc_6_i",
            "s8pc_5_i",
            "s8pc_4_h",
            "s8pc_3_h",
            "birthday0410_x",
            "joisc2021_a1",
            "xmascon16_b",
            "joisc2020_l",
            "joisc2021_a6",
            "joisc2020_o",
            "xmascon24_g",
            "apio_mansion",
            "joisc2020_m",
            "loadchecking_a",
            "iroha2019_day3_i",
            "utpc2012_06",
            "joisc2021_a4",
            "joisc2021_a2",
            "joisc2021_a3",
            "joisc2021_a5",
            "joisc2020_k",
            "utpc2014_f",
            "joisc2018_e",
            "geocon2013_a",
            "pakencamp_2018_day3_h",
            "tenka1_2016_final_b",
            "pakencamp_2020_day2_h",
            "pakencamp_2019_day4_h",
            "tenka1_2012_final_d",
            "joisc2008_election2",
            "tenka1_2013_qualB_e",
            "pakencamp_2019_day3_h"
        ]
        if problem_id in heuristic_list:
            problem_type = 1

        row = (
            problem_id,
            none_get(model.get("slope"), default_values["slope"]),
            none_get(model.get("intercept"), default_values["intercept"]),
            none_get(model.get("variance"), default_values["variance"]),
            none_get(model.get("difficulty"), default_values["difficulty"]),
            none_get(clip_difficulty, default_values["clip_difficulty"]),
            none_get(model.get("discrimination"), default_values["discrimination"]),
            none_get(model.get("irt_loglikelihood"), default_values["irt_loglikelihood"]),
            none_get(model.get("irt_users"), default_values["irt_users"]),
            none_get(model.get("is_experimental"), default_values["is_experimental"]),
            problem.get("contest_id"),
            problem.get("problem_index"),
            problem.get("name"),
            problem.get("title"),
            problem_type,
            contest_type
        )

        rows_to_insert.append(row)

    client.insert(
        table="atcoder.problem_models",
        data=rows_to_insert,
        column_names=["problem_id", "slope", "intercept", "variance", "difficulty", "clip_difficulty", "discrimination",
                      "irt_loglikelihood", "irt_users", "is_experimental", "contest_id", "problem_index", "name",
                      "title", "problem_type", "contest_type"]
    )

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

    parser_insert = subparsers.add_parser("complete", help="テーブルに Complete 処理を走らせる")
    parser_insert.set_defaults(func=complete_tables)

    parser_insert = subparsers.add_parser("update_problems", help="problem modelテーブルを最新に更新する")
    parser_insert.set_defaults(func=update_problem_models)

    parser_insert = subparsers.add_parser("update_rating", help="レーティングテーブル更新")
    parser_insert.set_defaults(func=update_rating_history)

    parser_insert = subparsers.add_parser("insert_contest", help="コンテストテーブル更新")
    parser_insert.set_defaults(func=insert_contest_models)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
