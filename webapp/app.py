import json
import time
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, send_from_directory, request, Response, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import utils

app = Flask(__name__, static_folder='frontend/build')
limiter = Limiter(app=app, key_func=get_remote_address, default_limits=["400 per day", "100 per hour"])
executor = ThreadPoolExecutor(1)
utils.init_vectordb()


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    if not path.startswith("static"):
        return send_from_directory(app.static_folder, 'index.html')
    else:
        return send_from_directory(app.static_folder, path)


@app.route('/api/search')
@limiter.limit("10/hour")  # Limit to 10 requests per hour for this route
def search():
    question = request.args.get('question', '')  # Get search query parameter
    print(f'Question received: {question}')
    description = f"Failed to answer question \"{question}\""
    try:
        docs = utils.query_vector_db(question, top_n=10)
        refs = {}
        for doc in docs:
            repo = doc.metadata['full_name']
            if repo not in refs:
                refs[repo] = ""
            refs[repo] += doc.page_content

        refs = '\n\n\n'.join([f"repository {k}: {v}" for k, v in refs.items()])

        sql = utils.question2sql(utils.load_tables_schema(), question)
        db_res = None
        if not (sql is None or sql.isspace()):
            db_res = utils.execute(sql)

        if db_res is None and refs is None:
            return f"Failed to answer question \"{question}\""

        description = utils.describe(question, sql, db_res, refs)
        print(f'Answer:\n{description}')
    except Exception as ex:
        print(f"Failed to answer question \"{question}\", exception: {ex}")

    return description


@app.route('/api/load_repos', methods=['POST'])
@limiter.limit("10/hour")
def load_repos():
    repo_list = request.get_json()['items']  # Get search query parameter

    def load_repos():
        for repo in repo_list:
            utils.load_repo(repo)
            time.sleep(1)
        utils.backup_vectordb()

    executor.submit(load_repos)
    return Response(), 200


@app.route('/api/summarize')
@limiter.limit("10/hour")  # Limit to 10 requests per hour for this route
def summarize():
    repo = request.args.get('repo', '')  # Get search query parameter
    print(f"summarizing {repo}")
    res = utils.summarize_repo(repo)
    print(res)
    return json.dumps(res)


if __name__ == '__main__':
    app.run(use_reloader=True, port=8008, threaded=True)
