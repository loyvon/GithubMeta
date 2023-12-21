import gzip
import json
import os
import tempfile
import zlib
from datetime import datetime

import requests
from requests.auth import HTTPBasicAuth
import time

from openai import AzureOpenAI, OpenAI
import mysql.connector
from config import Configuration


def get_openai_client():
    if Configuration.OpenaiApiType == "azure":
        client = AzureOpenAI(api_version=Configuration.OpenaiApiVersion,
                             api_key=Configuration.OpenaiApiKey.strip(),
                             azure_endpoint=Configuration.OpenaiAzureEndpoint.strip())
    else:
        client = OpenAI(api_key=Configuration.OpenaiApiKey.strip())
    return client


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


def load_topic(topic):
    conn = get_db()
    headers = {'Accept': 'application/vnd.github+json'}
    page_id = 1
    while True:
        url = ("https://api.github.com/search/repositories?"
               "q={}+in%3Atopics&sort=stars&order=desc&page={}&per_page=100"
               .format(topic, page_id))
        response = requests.get(url,
                                auth=HTTPBasicAuth(Configuration.GithubUsername, Configuration.GithubToken),
                                headers=headers)
        if not response.ok:
            print(response.text)
            break
        for data in json.loads(response.text)["items"]:
            load_repo_into_db(conn, data)
        print("Dumped page {} {}".format(topic, page_id))
        page_id += 1
        time.sleep(2)
    conn.commit()
    close_db(conn)
    print(f"Finished dumping pages for {topic}")


def load_activity(date, ofilepath):
    """Load the github activities of the given day."""
    url = f"https://data.gharchive.org/{date}-0.json.gz"
    print(url)
    remote_file = requests.get(url, stream=True)
    if not remote_file.ok:
        print(remote_file.text)
        return
    # Using zlib.MAX_WBITS|32 apparently forces zlib to detect the appropriate header for the data
    decompressor = zlib.decompressobj(zlib.MAX_WBITS | 32)
    # Stream this file in as a request - pull the content in just a little at a time
    with open(ofilepath, 'wb') as output:
        # Chunk size can be adjusted to test performance
        for chunk in remote_file.iter_content(chunk_size=8192):
            # Decompress the current chunk
            decompressed_chunk = decompressor.decompress(chunk)
            output.write(decompressed_chunk)




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
              "Query: ".format(schemas, question))
    print(prompt)
    response = get_openai_client().chat.completions.create(
        model=Configuration.OpenaiModel,
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


def describe(question, rows):
    prompt = ("Here is a question to answer: ```{}```\n"
              "And here is the query result that contains the answer: ```{}```.\n"
              "Please describe the rows in a way that answers the question, and use abbreviation when necessary "
              "to limit the response in 300 words."
              "Your description should focus on the question and the answer to the question."
              "Don't mention how the result generated as the description will be presented to end users."
              "Description: ").format(question, rows)
    print(prompt)
    response = get_openai_client().chat.completions.create(model=Configuration.OpenaiModel,
                                                           messages=[{"role": "system",
                                                                      "content": "You are an assistant that explains query results to people."},
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
    repos = set()
    url = f"https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
    filename = os.path.join(tempfile.gettempdir(), os.path.basename(url))
    print(f"Downloading {url}...")
    download_file(url, filename)
    with gzip.open(filename, 'rt') as f:
        for line in f:
            data = json.loads(line)
            repo = data['repo']['name']
            repos.add(repo)
    os.remove(filename)
    return repos


def load_active_repos(date: datetime):
    conn = get_db()
    repos = retrieve_repos(date.year, date.month, date.day, date.hour)
    for repo in repos:
        print(f"repo name: {repo}")
        url = f"https://api.github.com/repos/{repo}"
        headers = {'Accept': 'application/vnd.github+json'}
        response = requests.get(url,
                                auth=HTTPBasicAuth(Configuration.GithubUsername, Configuration.GithubToken),
                                headers=headers)
        if not response.ok:
            print(response.text)
            continue
        load_repo_into_db(conn, json.loads(response.text))
        # rate limit from Github ReST API.
        time.sleep(1)
        conn.commit()
    close_db(conn)
