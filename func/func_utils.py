import json
import os
import requests
import tempfile
import gzip


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
