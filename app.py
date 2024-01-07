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
        sql = utils.question2sql(utils.load_tables_schema(), question)
        if sql is None or sql.isspace():
            return f"Failed to answer question \"{question}\""
        res = utils.execute(sql)
        description = utils.describe(question, sql, res)
        print(f'Answer:\n{description}')
    except Exception as ex:
        print(f"Failed to answer question \"{question}\": {ex}")

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
