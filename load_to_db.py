import json
import os
import sqlite3
from config import config


def load_to_db(dataRoot, conn):
    """Load the raw data into sqlite3 db."""
    # Connect to SQLite database (or create it if it doesn't exist)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS github  
                 (id INTEGER PRIMARY KEY, name TEXT, full_name TEXT, owner_id INTEGER, owner_login TEXT, owner_type TEXT, html_url TEXT,
                  description TEXT, created_at TEXT, updated_at TEXT, pushed_at TEXT, clone_url TEXT, size INTEGER, 
                  stargazers_count INTEGER, watchers_count INTEGER, language TEXT, has_issues BOOLEAN, has_projects BOOLEAN,
                   has_downloads BOOLEAN, has_wiki BOOLEAN, has_pages BOOLEAN, has_discussions BOOLEAN, forks_count INTEGER,
                    archived BOOLEAN, disabled BOOLEAN, open_issues_count INTEGER, license TEXT, allow_forking BOOLEAN, 
                    is_template BOOLEAN, topics TEXT, visibility TEXT, forks INTEGER, open_issues INTEGER, 
                    watchers INTEGER, default_branch TEXT, score REAL)''')

    for dirName, subdirList, fileList in os.walk(dataRoot):
        print(f'Found directory: {dirName}')
        for fname in fileList:
            if fname.endswith('.DS_Store'):
                continue
            full_path = os.path.join(dirName, fname)
            print(f'\tFull path: {full_path}')
            with open(full_path, 'r') as f:
                full_data = json.load(f)
                for data in full_data['items']:
                    conn.execute(
                        "INSERT OR IGNORE INTO github VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (data['id'], data['name'], data['full_name'], data['owner']['id'], data['owner']['login'],
                         data['owner']['type'], data['html_url'], data['description'], data['created_at'],
                         data['updated_at'],
                         data['pushed_at'], data['clone_url'], data['size'], data['stargazers_count'],
                         data['watchers_count'],
                         data['language'], data['has_issues'], data['has_projects'], data['has_downloads'],
                         data['has_wiki'],
                         data['has_pages'], data['has_discussions'], data['forks_count'], data['archived'],
                         data['disabled'],
                         data['open_issues_count'], data['license']['key'] if data['license'] is not None else None,
                         data['allow_forking'], data['is_template'],
                         ', '.join(data['topics']), data['visibility'], data['forks'], data['open_issues'],
                         data['watchers'],
                         data['default_branch'], data['score']))
    conn.commit()


if __name__ == "__main__":
    dataRoot = os.path.join(config["data_dir"], "raw")
    db_path = os.path.join(config["data_dir"], "repos.db")
    conn = sqlite3.connect(db_path)
    load_to_db(dataRoot, conn)
    conn.close()
