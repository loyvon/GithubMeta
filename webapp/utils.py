import hashlib
import json
import os
import tempfile
import openai
import requests
from requests.auth import HTTPBasicAuth
import mysql.connector
from config import Configuration
from langchain_community.embeddings import AzureOpenAIEmbeddings
from langchain_community.vectorstores import DeepLake
from langchain.text_splitter import CharacterTextSplitter
from langchain_community.document_loaders import UnstructuredMarkdownLoader

openai.api_type = Configuration.OpenaiApiType
openai.api_base = Configuration.OpenaiAzureEndpoint.strip()
openai.api_version = Configuration.OpenaiApiVersion
openai.api_key = Configuration.OpenaiApiKey.strip()

embedding_function = AzureOpenAIEmbeddings(azure_endpoint=Configuration.OpenaiAzureEndpoint,
                                           azure_deployment="text-embedding-ada-002",
                                           api_key=Configuration.OpenaiApiKey)


def init_vectordb():
    return DeepLake(embedding_function=embedding_function,
                    dataset_path="az://githubvectordb/vectordb/github")


vectordb = init_vectordb()


def backup_vectordb():
    vectordb.ds.flush()


def load_into_vector_db(repo, readme):
    repo_name = repo['full_name']
    readme_md5 = hashlib.md5(readme.encode(encoding='UTF-8', errors='strict')).hexdigest()
    repo['readme_md5'] = readme_md5
    loaded_repo = vectordb.ds().filter(lambda sample: sample.meta['readme_md5'] == readme_md5)
    if loaded_repo.num_samples > 0:
        # Only update metadata
        loaded_repo[0].update(repo)
        print(f"readme of {repo_name} has no update.")
        return

    folder = os.path.join(tempfile.gettempdir(), repo_name)
    if not os.path.exists(folder):
        os.makedirs(folder)
    path = os.path.join(folder, 'readme.md')
    with open(path, 'w') as f:
        f.write(readme)

    loader = UnstructuredMarkdownLoader(path)
    documents = loader.load()
    text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=50)
    docs = text_splitter.split_documents(documents)
    for _ in docs:
        _.metadata = repo

    trial = 0
    while trial < 5:
        try:
            # At most 5 chunks, to avoid too much load on the embedding server
            vectordb.add_documents(documents=docs[:5])  # (ids=[repo_name], metadatas=[{"repo": repo_name}], texts=docs)
            break
        except openai.error.RateLimitError as err:
            print(err)
        trial += 1
    os.remove(path)


def query_vector_db(query, filter=None, top_n=1):
    trial = 0
    while trial < 5:
        try:
            docs = vectordb.similarity_search(query, filter=filter)
            return docs[:top_n]
        except openai.error.RateLimitError as err:
            print(err)
        trial += 1
    return None


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
                        watchers INTEGER, default_branch TEXT, score REAL, extra TEXT)''')
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
        "watchers, default_branch, score, extra)"
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
        "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (data['id'], data['name'], data['full_name'], data['owner']['id'], data['owner']['login'],
         data['owner']['type'], data['html_url'], ' '.join(data['description'].split()[:50]), data['created_at'],
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
         data['score'] if 'score' in data is not None else None,
         data['extra']))


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
              "And please only get the relevant columns from the tables, usually 5 columns is preferred."
              "And please always shorten the description (column `description`) in the result to within 50 words."
              "And please always order by stargazers_count DESC and limit 10"
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
    sql = msg.strip('<').strip('>').strip('`')
    print(f"Generated query: {sql}")
    return sql


def execute(query):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(query)
    res = cur.fetchall()
    close_db(conn)
    return res


def describe(question, query, rows, references):
    prompt = ("Here is a question to answer: ```{}```\n"
              "Here is the query: ```{}```\n"
              "And here is the query result that contains the answer: ```{}```.\n"
              "And here are the references, each belong to a specific repo: ```{}```.\n"
              "Please answer the question with the above information, and use abbreviation when necessary "
              "to limit the response in 1000 words."
              "Your description should focus on the question and the answer to the question."
              "You should follow the following requirements:"
              "1. Generate the description in markdown format."
              "2. The first part is the query result in table format if query result is not empty."
              "3. The second part is a brief explanation of the table content."
              "4. Don't mention the query, focus on question and result."
              "Description: ").format(question, query, rows, references)
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


def load_repo(repo_name):
    repo = get_repo(repo_name)
    if repo is None:
        return
    extra = get_extra_info(repo_name)
    readme = get_readme(repo_name, repo['default_branch'])
    load_into_vector_db(repo_name, readme)
    repo["extra"] = json.dumps(extra)
    conn = get_db()
    load_repo_into_db(conn, repo)
    conn.commit()
    close_db(conn)
    print(f'loaded repo: {repo_name}')


def get_repo(repo_name):
    url = f"https://api.github.com/repos/{repo_name}"
    return json.loads(get(url))


def get_readme(repo_name, default_branch):
    url = f"https://raw.githubusercontent.com/{repo_name}/{default_branch}/README.md"
    return get(url)


def get(url):
    headers = {'Accept': 'application/vnd.github+json'}
    response = requests.get(url,
                            auth=HTTPBasicAuth(Configuration.GithubUsername, Configuration.GithubToken),
                            headers=headers)
    if not response.ok:
        print(response.text)
        return None
    return response.text


def summarize_repo(repo_name):
    """
    Basic info such as stargazers, watchers, ...
    Used language.
    (Imported libraries: number of third-party libs.)
    (Doc: summary of readme and wiki.)
    Committers: top 10 contributors.
    Recent activities: number of PRs and issues closed last week/month.
    A quality rate.
    """
    query = f"SELECT * FROM repos WHERE full_name = '{repo_name}'"
    data = execute(query)
    res = {}
    if data is not None:
        repo = data[0]
        if repo[-1] is None:
            extra = {'top-languages': [], 'top-contributors': []}
        else:
            extra = json.loads(repo[-1])
        res["Name"] = repo[1]
        res["Full name"] = repo[2]
        res["Owner"] = repo[4]
        res["License"] = repo[26]
        res["Description"] = repo[7]
        res["Created at"] = repo[8]
        res["Most recent up at"] = repo[9]
        res["Stars"] = repo[13]
        res["Forks"] = repo[22]
        res["Main languages"] = f"{','.join(extra['top-languages'])}"
        res["Open issues count"] = repo[25]
        res["Topics"] = repo[29]
        res["Main contributors"] = f"{','.join(extra['top-contributors'])}"
    return res


def summarize_user(user_name):
    """
    TODO
    """


def get_extra_info(repo):
    extra = {}
    repo_name = repo['full_name']
    languages = json.loads(get(f"https://api.github.com/repos/{repo_name}/languages"))
    contributors = json.loads(get(f"https://api.github.com/repos/{repo_name}/contributors"))
    extra['top-languages'] = [k for k, v in languages.items()]
    extra['top-contributors'] = [item['login'] for item in contributors]
    return extra
