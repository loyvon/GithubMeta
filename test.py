import datetime

import requests

import utils


def test_e2e():
    question = "Which repository has the most stargazers?"
    sql = utils.question2sql(utils.load_tables_schema(), question)
    res = utils.execute(sql)
    print(utils.describe(question, res))


def test_load_repo():
    headers = { "contentType": "application/json"}
    payload = {"items": ["oliverklee/ext-oelib"]}
    requests.post("http://127.0.0.1:8008/api/load_repos", headers=headers, json=payload)


if __name__ == "__main__":
    # utils.init_db()
    test_load_repo()
    # test_e2e()
