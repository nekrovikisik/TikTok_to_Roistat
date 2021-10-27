import telebot
import pandas as pd
import requests
import json
import dateutil
from urllib.parse import urlencode, urlunparse
from six import string_types
from datetime import datetime
import schedule
from threading import Thread
from time import sleep
import os

#####   TIKTOK KEYS #####

token = os.getenv("TOKEN_TIKTOK")
advertiser_id = os.getenv("ADVERTISER_ID")

#####   ROISTAT KEYS #####
roistat_key = os.getenv("ROISTAT_KEY")
project = os.getenv("ROISTAT_PROJECT") # id проекта МР в Ройстате


METRICS = [
    "campaign_name", # название кампании
    "adgroup_name", # название группы объявлений
    "ad_name", # название объявления
    "spend", # потраченные деньги (валюта задаётся в рекламном кабинете)
    ]

tg_token = os.getenv("TG_TOKEN")
bot = telebot.TeleBot(tg_token)
chat_ids = []


@bot.message_handler(commands=['start'])
def start_message(message):
    chat_ids.append(message.chat.id)
    bot.send_message(message.chat.id,
                     'Привет. Я бот, который репортит об ошибках автоматической загрузки расходов. \n Еще ты можешь посмотреть список всех расходов.')


@bot.message_handler(content_types=['all_costs'])
def send_costs(message):
    bot.send_message(message.chat.id, 'costs')


def createArgs():
    d = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    date_from_dt = d - dateutil.relativedelta.relativedelta(days=30)
    date_from = date_from_dt.strftime("%Y-%m-%d")
    date_to_dt = d - dateutil.relativedelta.relativedelta(days=1)
    date_to = date_to_dt.strftime("%Y-%m-%d")

    args = {
        "metrics": METRICS, # список метрик, описанный выше
        "data_level": "AUCTION_AD", # тип рекламы
        "start_date": date_from, # начальный день запроса
        "end_date": date_to, # конечный день запроса
        "page_size": 1000, # размер страницы - количество объектов, которое возвращается за один запрос
        "page": 1, # порядковый номер страницы (если данные не поместились в один запрос, аргумент инкрементируется)
        "advertiser_id": advertiser_id, # один из ID из advertiser_ids, который мы получили при генерации access token
        "report_type": "BASIC", # тип отчета
        "dimensions": ["ad_id", "stat_time_day"] # аргументы группировки, вплоть до объявления и за целый день
    }
    return args

def build_url(args: dict) -> str:
    query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in args.items()})
    scheme = "https"
    netloc = "ads.tiktok.com"
    path = "/open_api/v1.2/reports/integrated/get/"
    return urlunparse((scheme, netloc, path, "", query_string, ""))

def get(args: dict, access_token: str) -> dict:
    url = build_url(args)
    headers = {
        "Access-Token": access_token,
    }
    rsp = requests.get(url, headers=headers)
    return rsp.json()

def getTikTok_costs(token, advertiser_id):
    args = createArgs()
    req = get(args, token)
    tiktok_costs = pd.json_normalize(req['data']['list'])
    tiktok_costs.columns = [i.replace('metrics.', '').replace('dimensions.', '') for i in tiktok_costs.columns]
    tiktok_costs['stat_time_day'] = pd.to_datetime(tiktok_costs['stat_time_day'], utc=True)
    tiktok_costs['stat_time_day'] = tiktok_costs['stat_time_day'].apply(lambda s: pd.Timestamp(s).timestamp())
    tiktok_costs['spend'] = tiktok_costs['spend'].astype(float)
    return tiktok_costs

def getRoistatCosts(projectID, name):
    params = {'project' : projectID, 'key' : roistat_key}
    res = requests.post('https://cloud.roistat.com/api/v1/project/analytics/source/cost/list', params=params)
    roistat_costs = pd.json_normalize(res.json(), 'data')
    roistat_costs['project'] = name

    roistat_costs = roistat_costs[roistat_costs['name'] == 'tiktok']
    roistat_costs['from_date'] = roistat_costs['from_date'].apply(lambda s: pd.Timestamp(s).timestamp())
    roistat_costs['to_date'] = roistat_costs['to_date'].apply(lambda s: pd.Timestamp(s).timestamp())
    return roistat_costs


def add_cost_in_roistat(tiktok_date):
    tiktok_costs = getTikTok_costs(token, advertiser_id)
    raw = tiktok_costs[tiktok_costs['stat_time_day'] == tiktok_date]
    costs = raw['spend']
    tiktok_date = pd.to_datetime(tiktok_date, unit='s');
    cost_sum = 0

    from datetime import datetime
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    today.utcnow()
    if tiktok_date >= today:
        return
    if costs.shape[0] >= 1:
        for cost in costs:
            cost_sum += cost

    base_url = 'https://cloud.roistat.com/api/v1/'
    auth_ = f'?key={roistat_key}&project={project}'
    add_cost = 'project/analytics/source/cost/add'
    url = f'{base_url}{add_cost}{auth_}'

    body = {
        "source": ":utm:tiktok",
        "from_date": str(tiktok_date),
        "to_date": str(tiktok_date),
        "timezone": "Europe/Moscow",
        "marketing_cost": cost_sum
    }
    res = requests.post(url=url, json=body)
    if res.json()['status'] == 'error':
        for chat_id in chat_ids:
            bot.send_message(chat_id, res.text + '\n\n' + raw)

def date_between(raw, tiktok_date):
    raw = pd.DataFrame(raw).T
    from_date = raw['from_date'].item(); to_date = raw['to_date'].item()
    if (tiktok_date <= to_date) and (tiktok_date >= from_date):
        return True
    add_cost_in_roistat(tiktok_date)
    return False


def is_ad_in_roistat(raw):
    roistat_costs = getRoistatCosts(project, "MR")
    return roistat_costs.apply(date_between, args=[raw['stat_time_day']], axis=1).any()


def schedule_checker():
    while True:
        schedule.run_pending()
        sleep(1)


def run():
    tiktok_costs = getTikTok_costs(token, advertiser_id)
    tiktok_costs.apply(is_ad_in_roistat, axis=1)


if __name__ == '__main__':
    schedule.every().day.at("16:00").do(run)
    Thread(target=schedule_checker).start()
    run()

