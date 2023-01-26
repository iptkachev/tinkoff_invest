import datetime
import pandas as pd
import dataframe_image
import os
from telegram import Bot
from telegram.constants import PARSEMODE_MARKDOWN
from tinvest import SyncClient
from logger import log_start_end
from stats import get_stats_for_period, _get_top_std_top_incr, _get_top_incr

TIMEOUT = 3

USD = "usd"
RUB = "rub"
os.environ["TELEGRAM_BOT_TOKEN"] = "5005286891:AAH23q2VYZyC_m4-814i9SGvE0X0RVgURUM"
os.environ["OPEN_API_TOKEN"] = "t.4MEsMwuaM45iHDQ4T217IhAFe6z7IUUMkQNJO1rkFbvJpv2HMmufAO8kqg86zJ4r4VzdrKQOxdZ9UQhN4Ps5GQ"
TINKOFF_OPEN_API_TOKEN = "OPEN_API_TOKEN"
TELEGRAM_BOT_TOKEN = "TELEGRAM_BOT_TOKEN"
# INVEST_CLUB_CHAT_ID = "-1001701958454"
INVEST_CLUB_CHAT_ID = 443842630  # ilya tkachev


@log_start_end
def etf_stats_job():
    bot = Bot(token=os.getenv(TELEGRAM_BOT_TOKEN))
    client = SyncClient(os.getenv(TINKOFF_OPEN_API_TOKEN))
    top_std = 20
    top_incr = 5
    days_to_analyze = 60
    date_to = datetime.datetime.now()
    date_from = date_to - datetime.timedelta(days=days_to_analyze)

    etfs = client.get_market_etfs()
    grouped_by_figi_stats = get_stats_for_period(client, etfs, date_from, date_to)
    min_std_max_incr_usd = _get_top_std_top_incr(grouped_by_figi_stats, USD, top_std, True, top_incr, False)
    min_std_max_incr_rub = _get_top_std_top_incr(grouped_by_figi_stats, RUB, top_std, True, top_incr, False)

    max_std_max_incr_usd = _get_top_std_top_incr(grouped_by_figi_stats, USD, top_std, False, top_incr, False)
    max_std_max_incr_rub = _get_top_std_top_incr(grouped_by_figi_stats, RUB, top_std, False, top_incr, False)

    max_incr_usd = _get_top_incr(grouped_by_figi_stats, USD, top_incr, False)
    max_incr_rub = _get_top_incr(grouped_by_figi_stats, RUB, top_incr, False)

    welcome_message = f"*Здоров, котаны. Свежая статистика по ETF за период {date_from.date()} - {date_to.date()}:*"
    bot.send_message(text=welcome_message, parse_mode=PARSEMODE_MARKDOWN, chat_id=INVEST_CLUB_CHAT_ID, timeout=TIMEOUT)

    rub = [
        (f"\n\nRUB Min std {top_std}, max incr", min_std_max_incr_rub),
        (f"\n\nRUB Max std {top_std}, max incr", max_std_max_incr_rub),
        (f"\n\nRUB Max incr", max_incr_rub),
    ]
    bot.send_message(text="*Статистика по rub фондам:*", parse_mode=PARSEMODE_MARKDOWN, chat_id=INVEST_CLUB_CHAT_ID, timeout=TIMEOUT)
    for message, stats in rub:
        _send_screenshot_with_caption(bot, message, stats)

    usd = [
        (f"\n\nUSD Min std {top_std}, max incr", min_std_max_incr_usd),
        (f"\n\nUSD Max std {top_std}, max incr", max_std_max_incr_usd),
        (f"\n\nUSD Max incr", max_incr_usd),
    ]
    bot.send_message(text="*Статистика по usd фондам:*", parse_mode=PARSEMODE_MARKDOWN, chat_id=INVEST_CLUB_CHAT_ID, timeout=TIMEOUT)
    for message, stats in usd:
        _send_screenshot_with_caption(bot, message, stats)


@log_start_end
def stocks_stats_job():
    bot = Bot(token=os.getenv(TELEGRAM_BOT_TOKEN))
    client = SyncClient(os.getenv(TINKOFF_OPEN_API_TOKEN))
    top_incr = 10
    days_to_analyze = 5
    date_to = datetime.datetime.now()
    date_from = date_to - datetime.timedelta(days=days_to_analyze)

    etfs = client.get_market_stocks()
    grouped_by_figi_stats = get_stats_for_period(client, etfs, date_from, date_to, sleep_seconds=60)

    max_incr_usd = _get_top_incr(grouped_by_figi_stats, USD, top_incr, False)
    max_incr_rub = _get_top_incr(grouped_by_figi_stats, RUB, top_incr, False)

    welcome_message = f"*Здоров, котаны. Свежая статистика по акциям(топ-{top_incr})" \
                      f"за период {date_from.date()} - {date_to.date()}:*"
    bot.send_message(text=welcome_message, parse_mode=PARSEMODE_MARKDOWN, chat_id=INVEST_CLUB_CHAT_ID, timeout=TIMEOUT)

    _send_screenshot_with_caption(bot, f"\n\nRUB Max incr", max_incr_rub)
    _send_screenshot_with_caption(bot, f"\n\nUSD Max incr", max_incr_usd)


def _send_screenshot_with_caption(bot: Bot, message: str, stats: pd.DataFrame):
    png = "stats.png"
    dataframe_image.export(stats, png, table_conversion="matplotlib")
    with open(png, "rb") as file:
        bot.send_photo(photo=file, caption=f"_{message}_", parse_mode=PARSEMODE_MARKDOWN, chat_id=INVEST_CLUB_CHAT_ID, timeout=TIMEOUT)
    os.remove(png)
