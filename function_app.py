import json

import azure.functions as func
import datetime
import logging
import requests
import utils

app = func.FunctionApp()


@app.timer_trigger(schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def LoadRepos(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    date = datetime.datetime.now() - datetime.timedelta(hours=2)
    repos = utils.retrieve_repos(date.year, date.month, date.day, date.hour)
    payload = json.dumps({
            "items": repos
        })
    response = requests.post("https://githubmeta.azurewebsites.net/api/load_repos", json=payload)
    logging.info(response.text)
    logging.info('Python timer trigger function executed.')