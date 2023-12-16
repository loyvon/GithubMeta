import os
import sqlite3

from flask import Flask, send_from_directory, request
from flask_cors import CORS

import nlp2sql
from config import config
from load_to_db import load_to_db

app = Flask(__name__, static_folder='search/build')
CORS(app)

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    if path != "" and not path.startswith("static"):
        return send_from_directory(app.static_folder, 'index.html')
    else:
        return send_from_directory(app.static_folder, path)


@app.route('/api')
def api():
    search_term = request.args.get('search', '')  # Get search query parameter
    print(f'You searched for: {search_term}')
    db_path = os.path.join(config["data_dir"], "repos.db")
    conn = sqlite3.connect(db_path)
    sql = nlp2sql.question2sql(conn, search_term)
    if sql is None:
        return f"Failed to answer: {search_term}"
    res = nlp2sql.execute(conn, sql)
    description = nlp2sql.describe(search_term, res)
    conn.close()
    return description


if __name__ == '__main__':
    app.run(use_reloader=True, port=8008, threaded=True)
