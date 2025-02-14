#!/usr/bin/env python3
import argparse
import os
import time

import clickhouse_connect
import requests

def make_clickhouse_client():
    dsn = os.getenv("CLICKHOUSE_DSN")
    client = clickhouse_connect.get_client(dsn=dsn)
    return client

def complete_submissions(args):
    client = make_clickhouse_client()
    client.command(f"OPTIMIZE TABLE atcoder.submissions FINAL")


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
        headers = {"Accept-Encoding": "gzip, deflate, br"}
        response = requests.get(url, headers=headers)
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

        time.sleep(2)

def all_update_submissions(args):
    """
    1. ClickHouse から最初の unixtime を取得
    2. unixtime を使って API を叩く
    3. 取得データを ClickHouse に upsert
    4. 取得配列が空なら次の処理を停止
    5. 一番最後の unixtime で再度 API を叩く
    """

    client = make_clickhouse_client()

    current_unixtime = 0

    while True:
        print(f"Requesting with unixtime: {current_unixtime}")
        url = f"https://kenkoooo.com/atcoder/atcoder-api/v3/from/{current_unixtime}"
        headers = {"Accept-Encoding": "gzip, deflate, br"}
        response = requests.get(url, headers=headers)
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

        # Client.insert の使用例
        # 'atcoder.replaceable_submissions' テーブルのカラム名を指定して、データの挿入を実行します。
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

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
