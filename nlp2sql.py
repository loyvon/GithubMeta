import os
import re

from config import config
from openai import AzureOpenAI, OpenAI
import sqlite3

if config['openai_api_type'] == "azure":
    client = AzureOpenAI(api_version=config["openai_api_version"],
                         api_key=config["openai_api_key"].strip(),
                         azure_endpoint=config["openai_azure_endpoint"].strip())
else:
    client = OpenAI()


def load_tables_schema(conn):
    """Load the table schema as return the schema in text format."""
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = c.fetchall()
    table_schemas = []
    for table in tables:
        c.execute(f"PRAGMA table_info({table[0]})")
        schema = c.fetchall()
        table_schemas.append(f"Schema for {table[0]}: {','.join([_[1] for _ in schema])}")
    return ';'.join(table_schemas)


def question2sql(conn, question):
    prompt = ("SQLite3 SQL tables, with their schemas:\n\n{}"
              "\n\nA query to answer question: ```{}```\n\n"
              "Query: ".format(load_tables_schema(conn), question))
    print(prompt)
    response = client.chat.completions.create(model=config["openai_azure_deployment"],
                                              messages=[{"role": "system",
                                                         "content": "You are a database expert that helps people generate queries for their questions "
                                                                    "based on given table schemas."},
                                                        {"role": "user", "content": prompt}],
                                              temperature=0,
                                              max_tokens=1000,
                                              top_p=1,
                                              frequency_penalty=0,
                                              presence_penalty=0,
                                              stop=["#", ";"])
    msg = response.choices[0].message.content
    if msg.startswith("```sql"):
        return msg.strip("```sql")
    return None

def execute(conn, query):
    c = conn.cursor()
    c.execute(query)
    rows = c.fetchall()
    return rows

def describe(question, rows):
    prompt = ("Here is a question to answer: ```{}```\n"
              "And here is the query result that contains the answer: ```{}```.\n"
              "Please describe the rows in a way that answers the question, and use abbreviation when necessary "
              "to limit the response in 300 words.").format(question, rows)
    print(prompt)
    response = client.chat.completions.create(model=config["openai_azure_deployment"],
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


if __name__ == "__main__":
    DB_PATH = os.path.join(config["data_dir"], "repos.db")
    conn = sqlite3.connect(DB_PATH)
    while True:
        user_input = input("Please enter your question (or 'quit' to stop): ")
        if user_input.lower() == 'quit':
            break
        else:
            print(f"You entered: {user_input}")
            print(f"Generated sql: {question2sql(conn, user_input)}")
    conn.close()
