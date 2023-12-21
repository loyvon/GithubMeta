import datetime

import utils


def test_e2e():
    question = "Which repository has the most stargazers?"
    sql = utils.question2sql(utils.load_tables_schema(), question)
    res = utils.execute(sql)
    print(utils.describe(question, res))


def test_get_active_repo():
    utils.load_active_repos(datetime.datetime.now() - datetime.timedelta(days=1))


if __name__ == "__main__":
    # utils.init_db()
    utils.load_active_repos(datetime.datetime.now() - datetime.timedelta(hours=24))
