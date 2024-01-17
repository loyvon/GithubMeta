import hashlib
import json
import os
import tempfile
import traceback

import openai
import psycopg2
import requests
from requests.auth import HTTPBasicAuth
from config import Configuration
from langchain_community.embeddings import AzureOpenAIEmbeddings
from langchain.text_splitter import CharacterTextSplitter
from langchain_community.document_loaders import UnstructuredMarkdownLoader

openai.api_type = Configuration.OpenaiApiType
openai.api_base = Configuration.OpenaiAzureEndpoint.strip()
openai.api_version = Configuration.OpenaiApiVersion
openai.api_key = Configuration.OpenaiApiKey.strip()

embedding_function = AzureOpenAIEmbeddings(azure_endpoint=Configuration.OpenaiAzureEndpoint,
                                           azure_deployment="text-embedding-ada-002",
                                           api_key=Configuration.OpenaiApiKey)


def get_chunked_embeddings(repo, readme):
    repo_name = repo['full_name']
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
    texts = [_.page_content for _ in docs]
    os.remove(path)
    return zip(texts, embedding_function.embed_documents(texts, chunk_size=1000))


def get_db():
    conn = psycopg2.connect(user=Configuration.DbUser,
                            password=Configuration.DbPassword,
                            host=Configuration.DbHost,
                            port=5432,
                            database="githubmeta",
                            sslmode="require")
    return conn


def close_db(conn):
    conn.close()


def init_db():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('CREATE EXTENSION IF NOT EXISTS "vector";')
    cursor.execute('''CREATE TABLE IF NOT EXISTS repos  
                        (
                            id INTEGER PRIMARY KEY, 
                            name TEXT, 
                            full_name TEXT, 
                            owner_id INTEGER, 
                            owner_login TEXT, 
                            owner_type TEXT, 
                            html_url TEXT,
                            description TEXT, 
                            created_at TEXT, 
                            updated_at TEXT, 
                            pushed_at TEXT, 
                            clone_url TEXT, 
                            size INTEGER, 
                            stargazers_count INTEGER, 
                            watchers_count INTEGER, 
                            language TEXT, 
                            has_issues BOOLEAN, 
                            has_projects BOOLEAN,
                            has_downloads BOOLEAN, 
                            has_wiki BOOLEAN, 
                            has_pages BOOLEAN, 
                            has_discussions BOOLEAN, 
                            forks_count INTEGER,
                            archived BOOLEAN, 
                            disabled BOOLEAN, 
                            open_issues_count INTEGER, 
                            license TEXT, 
                            allow_forking BOOLEAN, 
                            is_template BOOLEAN, 
                            topics TEXT, 
                            visibility TEXT, 
                            forks INTEGER, 
                            open_issues INTEGER, 
                            watchers INTEGER, 
                            default_branch TEXT, 
                            score REAL, 
                            readme_md5 TEXT, 
                            extra TEXT
                        )
                        ''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS repo_readme_vector  
                        (
                            repo_id INTEGER,
                            chunk_id INTEGER,
                            text TEXT,
                            embedding vector(1536)
                        )
                        ''')
    cursor.execute("""DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1
                            FROM   pg_constraint 
                            WHERE  conname = 'repo_chunk_unique'
                        )
                        THEN
                            ALTER TABLE repo_readme_vector
                            ADD CONSTRAINT repo_chunk_unique UNIQUE (repo_id, chunk_id);
                        END IF;
                    END
                    $$;
                    """)
    cursor.execute("""
        create or replace function match_readme (
          query_embedding vector(1536),
          match_threshold float,
          match_count int
        )
        returns table (
          repo_id int,
          chunk_id int,
          text text,
          similarity float
        )
        language sql stable
        as $$
          select
            repo_readme_vector.repo_id,
            repo_readme_vector.chunk_id,
            repo_readme_vector.text,
            1 - (repo_readme_vector.embedding <=> query_embedding) as similarity
          from repo_readme_vector
          where repo_readme_vector.embedding <=> query_embedding < 1 - match_threshold
          order by repo_readme_vector.embedding <=> query_embedding
          limit match_count;
        $$;
        """)
    conn.commit()
    close_db(conn)


def load_repo_into_db(data):
    conn = get_db()
    try:
        cursor = conn.cursor()
        repo_col_list = ['id', 'name', 'full_name', 'owner_id', 'owner_login', 'owner_type', 'html_url',
                         'description', 'created_at', 'updated_at', 'pushed_at', 'clone_url', 'size',
                         'stargazers_count', 'watchers_count', 'language', 'has_issues', 'has_projects',
                         'has_downloads', 'has_wiki', 'has_pages', 'has_discussions', 'forks_count',
                         'archived', 'disabled', 'open_issues_count', 'license', 'allow_forking',
                         'is_template', 'topics', 'visibility', 'forks', 'open_issues',
                         'watchers', 'default_branch', 'score', 'readme_md5', 'extra']
        cursor.execute(
            "INSERT INTO repos "
            f"({','.join(repo_col_list)})"
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            "ON CONFLICT (id) DO UPDATE SET "
            f"{','.join([_ + ' = ' + 'excluded.' + _ for _ in repo_col_list])};",
            (data['id'], data['name'], data['full_name'], data['owner']['id'], data['owner']['login'],
             data['owner']['type'], data['html_url'],
             ' '.join(data['description'].split()[:50]) if data['description'] is not None else None,
             data['created_at'],
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
             data['readme_md5'],
             data['extra']))
        embeddings = data['readme']
        if embeddings is not None:
            for idx, embed in enumerate(embeddings):
                repo_readme_vector_col_list = ['repo_id', 'chunk_id', 'text', 'embedding']
                cursor.execute("INSERT INTO repo_readme_vector "
                               f"({','.join(repo_readme_vector_col_list)})"
                               "VALUES (%s, %s, %s, %s)"
                               "ON CONFLICT (repo_id, chunk_id) DO UPDATE SET "
                               f"{','.join([_ + ' = ' + 'excluded.' + _ for _ in repo_readme_vector_col_list])};",
                               (data['id'], idx, embed[0], embed[1]))
        conn.commit()
    finally:
        close_db(conn)


def load_tables_schema():
    """Load the table schema as return the schema in text format."""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT table_name, column_name"
                  " FROM information_schema.columns"
                  f" WHERE table_schema = 'githubmeta-database';")
        tables = c.fetchall()
        table_schemas = {}
        for row in tables:
            table_name = row[0]
            col_name = row[1]
            if table_name not in table_schemas.keys():
                table_schemas[table_name] = []
            table_schemas[table_name].append(col_name)
        schema = '\n'.join([f"table name: {k}, table columns: {','.join(v)}" for k, v in table_schemas.items()])
        return schema
    finally:
        close_db(conn)


def question2sql(schemas, question):
    prompt = ("Postgresql tables schemas:\n\n```{}```"
              "Function `match_readme` can be used to retrieve the relevant readme content of repos. "
              "If you want to search the readme for answer, call this function with 3 params: {}, 0.5, 10,"
              " the result of the function call is (repo id, readme chunk index, text of the chunk, similarity). "
              "You can use the result from the function to join `repos` for further analysis."
              "\n\nPlease generate a query to answer question: ```{}```\n\n"
              "Start the query with `<` and end the query with `>`, example: `<SELECT * FROM repos LIMIT 1>`."
              "And please only get the relevant columns from the tables, usually less than 10 columns is preferred."
              "And please always shorten the description (column `description`) in the result to within 50 words."
              "And please always order by stargazers_count DESC and limit 10"
              "If you are not sure how to generate the query, just respond `<>`"
              "Query: ".format(schemas,
                               embedding_function.embed_documents([question], chunk_size=1000)[0],
                               question))
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
    try:
        cur = conn.cursor()
        cur.execute(query)
        res = cur.fetchall()
        return res
    finally:
        close_db(conn)


def describe(question, query, rows):
    prompt = ("Here is a question to answer: ```{}```\n"
              "Here is the query: ```{}```\n"
              "And here is the query result that contains the answer: ```{}```.\n"
              "Please answer the question with the above information, and use abbreviation when necessary "
              "to limit the response in 1000 words."
              "Your description should focus on the question and the answer to the question."
              "You should follow the following requirements:"
              "1. Generate the description in markdown format."
              "2. The first part is the query result in table format if query result is not empty."
              "3. The second part is a brief explanation of the table content."
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


def load_repo(repo_name):
    repo = get_repo(repo_name)
    if repo is None:
        print(f"Didn't get repo {repo_name}")
        return
    extra = get_extra_info(repo_name)
    readme = get_readme(repo_name, repo['default_branch'])
    embeddings = None
    if readme is not None:
        readme_md5 = hashlib.md5(readme.encode(encoding='UTF-8', errors='strict')).hexdigest()
        repo['readme_md5'] = readme_md5
        repo['readme'] = None
        rows = execute(f"SELECT COUNT(*) FROM repos WHERE \"full_name\" = '{repo_name}' AND \"readme_md5\" = '{readme_md5}'")
        if rows[0][0] == 0:
            # make the readme more compact.
            readme += ' '.join(readme.split())
            embeddings = get_chunked_embeddings(repo, readme)
            repo['readme'] = embeddings
    else:
        repo['readme_md5'] = None
        repo['readme'] = None

    repo["extra"] = json.dumps(extra)
    load_repo_into_db(repo)
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


def get_extra_info(repo_name):
    extra = {}
    languages = json.loads(get(f"https://api.github.com/repos/{repo_name}/languages"))
    contributors = json.loads(get(f"https://api.github.com/repos/{repo_name}/contributors"))
    extra['top-languages'] = [k for k, v in languages.items()]
    extra['top-contributors'] = [item['login'] for item in contributors]
    return extra
