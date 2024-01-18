import urllib.parse
import requests
import utils


def test_e2e():
    question = "Which repository has most stargazers?"
    sql = utils.question2sql(utils.load_tables_schema(), question)
    res = utils.execute(sql)
    print(utils.describe(question, res))


def test_load_repo():
    headers = { "contentType": "application/json"}
    payload = {"items": ["tensorflow/tensorflow"]}
    requests.post("http://127.0.0.1:5000/api/load_repos", headers=headers, json=payload)


def summarize_repo():
    headers = {"contentType": "application/json"}
    repo = urllib.parse.quote('tensorflow/tensorflow', safe='')
    resp = requests.get(f"http://127.0.0.1:5000/api/summarize?repo={repo}", headers=headers)
    print(resp.text)


def test_vectordb():
    repo_name = "tensorflow/tensorflow"
    repo = utils.get_repo(repo_name)
    readme = utils.get_readme(repo_name, repo['default_branch'])
    res = utils.get_chunked_embeddings(repo, readme)
    for txt, embed in res:
        print(f"{txt}:{embed}")


if __name__ == "__main__":
    # utils.init_db()
    test_load_repo()
    test_e2e()
    # summarize_repo()
    test_vectordb()
