import azure.functions as func
import datetime
import logging
import utils

app = func.FunctionApp()


@app.timer_trigger(schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def LoadRepos(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    utils.load_active_repos(datetime.datetime.now() - datetime.timedelta(days=1))
    logging.info('Python timer trigger function executed.')
