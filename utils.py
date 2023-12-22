import gzip
import json
import os
import tempfile

import openai

import requests
from requests.auth import HTTPBasicAuth

import mysql.connector

from config import Configuration

openai.api_type = Configuration.OpenaiApiType
openai.api_base = Configuration.OpenaiAzureEndpoint.strip()
openai.api_version = Configuration.OpenaiApiVersion
openai.api_key = Configuration.OpenaiApiKey.strip()


def get_db():
    conn = mysql.connector.connect(pool_name="mypool",
                                   pool_size=10,
                                   host=Configuration.MysqlHost,
                                   user=Configuration.MysqlUser,
                                   password=Configuration.MysqlPasswd,
                                   database=Configuration.MysqlName,
                                   client_flags=[mysql.connector.ClientFlag.SSL],
                                   ssl_ca='./DigiCertGlobalRootCA.crt.pem',
                                   ssl_disabled=False,
                                   port=3306)
    return conn


def close_db(conn):
    conn.close()


def init_db():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS repos  
                     (id INTEGER PRIMARY KEY, name TEXT, full_name TEXT, owner_id INTEGER, owner_login TEXT, owner_type TEXT, html_url TEXT,
                      description TEXT, created_at TEXT, updated_at TEXT, pushed_at TEXT, clone_url TEXT, size INTEGER, 
                      stargazers_count INTEGER, watchers_count INTEGER, language TEXT, has_issues BOOLEAN, has_projects BOOLEAN,
                       has_downloads BOOLEAN, has_wiki BOOLEAN, has_pages BOOLEAN, has_discussions BOOLEAN, forks_count INTEGER,
                        archived BOOLEAN, disabled BOOLEAN, open_issues_count INTEGER, license TEXT, allow_forking BOOLEAN, 
                        is_template BOOLEAN, topics TEXT, visibility TEXT, forks INTEGER, open_issues INTEGER, 
                        watchers INTEGER, default_branch TEXT, score REAL)''')
    conn.commit()
    close_db(conn)


def load_repo_into_db(conn, data):
    cursor = conn.cursor()
    cursor.execute(
        "REPLACE INTO repos "
        "(id, name, full_name, owner_id, owner_login, owner_type, html_url, "
        "description, created_at, updated_at, pushed_at, clone_url, size, "
        "stargazers_count, watchers_count, language, has_issues, has_projects,"
        "has_downloads, has_wiki, has_pages, has_discussions, forks_count,"
        "archived, disabled, open_issues_count, license, allow_forking,"
        "is_template, topics, visibility, forks, open_issues,"
        "watchers, default_branch, score)"
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
        "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (data['id'], data['name'], data['full_name'], data['owner']['id'], data['owner']['login'],
         data['owner']['type'], data['html_url'], data['description'], data['created_at'],
         data['updated_at'],
         data['pushed_at'], data['clone_url'], data['size'], data['stargazers_count'],
         data['watchers_count'],
         data['language'], data['has_issues'], data['has_projects'], data['has_downloads'],
         data['has_wiki'],
         data['has_pages'], data['has_discussions'], data['forks_count'], data['archived'],
         data['disabled'],
         data['open_issues_count'],
         data['license']['key'] if data['license'] is not None else None,
         data['allow_forking'],
         data['is_template'],
         ', '.join(data['topics']), data['visibility'], data['forks'], data['open_issues'],
         data['watchers'],
         data['default_branch'],
         data['score'] if 'score' in data is not None else None))


def load_tables_schema():
    """Load the table schema as return the schema in text format."""
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT table_name, column_name"
              " FROM information_schema.columns"
              f" WHERE table_schema = 'github';")
    tables = c.fetchall()
    table_schemas = {}
    for row in tables:
        table_name = row[0]
        col_name = row[1]
        if table_name not in table_schemas.keys():
            table_schemas[table_name] = []
        table_schemas[table_name].append(col_name)

    close_db(conn)
    schema = '\n'.join([f"table name: {k}, table columns: {','.join(v)}" for k, v in table_schemas.items()])
    return schema


def question2sql(schemas, question):
    prompt = ("MySql tables schemas:\n\n```{}```"
              "\n\nPlease generate a query to answer question: ```{}```\n\n"
              "Start the query with `<` and end the query with `>`, example: `<SELECT * FROM repos LIMIT 1>`."
              "Requirement: At most 20 rows can be returned."
              "If you are not sure how to generate the query, just respond `<>`"
              "Query: ".format(schemas, question))
    print(prompt)
    response = openai.ChatCompletion.create(
        engine=Configuration.OpenaiModel,
        messages=[{"role": "system",
                   "content":
                       "You are a database expert that helps people generate queries for their questions "
                       "based on given table schemas."},
                  {"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=1000,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=["#", ";"]
    )
    msg = response.choices[0].message.content
    print(f'Msg from model: \n{msg}\n')
    sql = msg.strip('<').strip('>').strip('```').strip('sql')
    print(f"Generated query: {sql}")
    return sql


def execute(query):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(query)
    res = cur.fetchall()
    close_db(conn)
    return res


def describe(question, query, rows):
    prompt = ("Here is a question to answer: ```{}```\n"
              "Here is the query: ```{}```\n"
              "And here is the query result that contains the answer: ```{}```.\n"
              "Please describe the rows in a way that answers the question, and use abbreviation when necessary "
              "to limit the response in 300 words."
              "Your description should focus on the question and the answer to the question."
              "You should follow the following requirements:"
              "1. Generate the description in markdown format."
              "2. The first part is the query result in table format."
              "3. The second part is a very brief explanation of the table content."
              "4. Don't mention the query, focus on question and result."
              "Description: ").format(question, query, rows)
    print(prompt)
    response = openai.ChatCompletion.create(engine=Configuration.OpenaiModel,
                                            messages=[{"role": "system",
                                                       "content": "You are an assistant that answer questions for people."},
                                                      {"role": "user", "content": prompt}],
                                            temperature=0,
                                            max_tokens=1000,
                                            top_p=1,
                                            frequency_penalty=0,
                                            presence_penalty=0,
                                            stop=["#", ";"])
    msg = response.choices[0].message.content
    return msg


def download_file(url, filename):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def retrieve_repos(year, month, day, hour):
    """
    Retrieve repos active last week.
    """
    repos = {}
    url = f"https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
    filename = os.path.join(tempfile.gettempdir(), os.path.basename(url))
    print(f"Downloading {url}...")
    download_file(url, filename)
    with gzip.open(filename, 'rt') as f:
        for line in f:
            data = json.loads(line)
            if data['public'] and ((data['type'] == "PullRequestEvent" and data['payload']['action'] == 'closed')
                                   or (data['type'] == 'WatchEvent')):
                repo = data['repo']['name']
                if repo not in repos:
                    repos[repo] = 0
                repos[repo] += 1
    os.remove(filename)
    repos = [k for k, v in sorted(repos.items(), key=lambda item: -item[1])]
    return repos[:2000]  # Only first 2000 repos


def load_repo(repo_name):
    url = f"https://api.github.com/repos/{repo_name}"
    headers = {'Accept': 'application/vnd.github+json'}
    response = requests.get(url,
                            auth=HTTPBasicAuth(Configuration.GithubUsername, Configuration.GithubToken),
                            headers=headers)
    if not response.ok:
        print(response.text)
        return
    conn = get_db()
    load_repo_into_db(conn, json.loads(response.text))
    conn.commit()
    close_db(conn)
    print(f'loaded repo: {repo_name}')
