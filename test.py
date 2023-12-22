import datetime

import requests

import utils


def test_e2e():
    question = "Which repository has most stargazers?"
    sql = utils.question2sql(utils.load_tables_schema(), question)
    res = utils.execute(sql)
    print(utils.describe(question, res.to_csv(header=True, index=True)))

    try:
        import base64
        with open("chartn.png", "wb") as fh:
            chart, explanation = utils.describe_lida(res, question)
            fh.write(base64.decodebytes(chart.raster.encode()))
            print(explanation)
    except Exception as ex:
        print(ex)

def test_load_repo():
    headers = { "contentType": "application/json"}
    payload = {"items": ["oliverklee/ext-oelib"]}
    requests.post("http://127.0.0.1:8008/api/load_repos", headers=headers, json=payload)


if __name__ == "__main__":
    # utils.init_db()
    # test_load_repo()
    test_e2e()
