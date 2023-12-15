import requests
from requests.auth import HTTPBasicAuth
import time
from config import config

def get_repos():
    username = config['username']
    token = config['token']
    headers = {'Accept': 'application/vnd.github+json'}

    topics = ["database", "big-data", "data-analytics", "data-visualization", "programming-language", "distributed-system",
              "artificial-intelligence", "machine-learning", "deep-learning"]

    for topic in topics:
        page_id = 1
        while True:
            url = ("https://api.github.com/search/repositories?q={}+in%3Atopics&sort=stars&order=desc&page={}&per_page=100"
                .format(topic, page_id))
            response = requests.get(url, auth=HTTPBasicAuth(username, token), headers=headers)
            if not response.ok:
                print(response.text)
                break
            with open('{}/raw/{}_page_{:08d}.json'.format(config['data_dir'], topic, page_id), 'w') as f:
                f.write(response.text)
                f.write('\n')
                f.flush()
                print("Dumped page {} {}".format(topic, page_id))
            page_id += 1
            time.sleep(2)

    print("Finished dumping pages")


if __name__ == "__main__":
    get_repos()
