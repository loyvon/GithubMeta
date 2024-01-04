import datetime
import urllib.parse

import requests

import utils


def test_e2e():
    question = "Which repository has most stargazers?"
    sql = utils.question2sql(utils.load_tables_schema(), question)
    res = utils.execute(sql)
    print(utils.describe(question, sql, res))


def test_load_repo():
    headers = { "contentType": "application/json"}
    payload = {"items": ["oliverklee/ext-oelib"]}
    requests.post("http://127.0.0.1:8008/api/load_repos", headers=headers, json=payload)


def summarize_repo():
    headers = {"contentType": "application/json"}
    repo = urllib.parse.quote('oliverklee/ext-oelib', safe='')
    resp = requests.get(f"http://127.0.0.1:8008/api/summarize?repo={repo}", headers=headers)
    print(resp.text)


if __name__ == "__main__":
    # utils.init_db()
    # test_load_repo()
    # test_e2e()
    summarize_repo()
