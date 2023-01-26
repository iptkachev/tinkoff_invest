import datetime
import time
from collections import namedtuple
from typing import List
import pandas as pd
from tqdm import tqdm
import tinvest
from logger import get_logger

YEAR_DAYS = 365

Asset = namedtuple("Asset", ["name", "figi", "currency", "closed"])
ts_value = namedtuple("TsValue", ["dttm", "value"])


def get_stats_for_period(client: tinvest.SyncClient,
                         market_instruments: tinvest.MarketInstrumentListResponse,
                         date_from: datetime.datetime, date_to: datetime.datetime,
                         sleep_seconds=0) -> pd.DataFrame:
    assets_df = get_assets_df_for_period(client, market_instruments, date_from, date_to, sleep_seconds)
    grouped_by_figi_stats = _make_group_by_figi_from_df_by_period(assets_df)
    return grouped_by_figi_stats


def get_assets_df_for_period(client: tinvest.SyncClient,
                             market_instruments: tinvest.MarketInstrumentListResponse,
                             date_from: datetime.datetime, date_to: datetime.datetime,
                             sleep_seconds=0) -> pd.DataFrame:
    assets = list(map(lambda inst: Asset(inst.name, inst.figi, inst.currency.name, []),
                      market_instruments.payload.instruments))
    logger = get_logger()

    counter_requests = 0
    max_requests_per_minute = 100  # from user experience
    for a in tqdm(assets, total=len(assets)):
        intervals = _cut_period_for_year_intervals(date_from, date_to)
        for j, interval_date_to in enumerate(intervals[1:], 1):
            try:
                interval_date_from = intervals[j - 1]
                a.closed.extend(
                    list(map(
                        lambda x: ts_value(x.time.date(), float(x.c)),
                        client.get_market_candles(
                            a.figi,
                            from_=interval_date_from,
                            to=interval_date_to,
                            interval=tinvest.schemas.CandleResolution.day
                        ).payload.candles
                    ))
                )
            except tinvest.exceptions.UnexpectedError:
                logger.error("Can't execute 'get_market_candles' with args:")
                logger.error(f"{a}, {interval_date_from}, {interval_date_to}")
            except tinvest.exceptions.TooManyRequestsError:
                logger.error(f"TooManyRequestsError (counter={counter_requests}) 'get_market_candles' with args:")
                logger.error(f"{a}, {interval_date_from}, {interval_date_to}")           
            finally:
                counter_requests += 1
                if counter_requests % max_requests_per_minute == 0 and counter_requests >= max_requests_per_minute:
                    time.sleep(sleep_seconds)

    return _make_df_by_period_from_assets(assets)


def _get_top_std_top_incr(grouped_by_figi_stats: pd.DataFrame,
                          currency: str,
                          top_std: int,
                          ascending_std: bool,
                          top_incr: int,
                          ascending_incr: bool) -> pd.DataFrame:
    return (
        grouped_by_figi_stats[grouped_by_figi_stats.currency == currency]
        .sort_values('std_incr_rate', ascending=ascending_std)
        .head(top_std)
        .sort_values('total_incr', ascending=ascending_incr)
        .head(top_incr)
    )


def _get_top_incr(grouped_by_figi_stats: pd.DataFrame,
                  currency: str,
                  top_incr: int,
                  ascending_incr: bool) -> pd.DataFrame:
    return (
        grouped_by_figi_stats[grouped_by_figi_stats.currency == currency]
        .sort_values('total_incr', ascending=ascending_incr)
        .head(top_incr)
    )


def _make_df_by_period_from_assets(assets: List[Asset]) -> pd.DataFrame:
    assets_df = pd.DataFrame(assets).dropna()
    assets_df = assets_df[assets_df.closed.apply(len) > 1]
    assets_df = assets_df.explode('closed')
    assets_df = assets_df.reset_index(drop=True)
    assets_df['dt'] = assets_df['closed'].apply(lambda x: x.dttm if not pd.isnull(x) else x)
    assets_df['closed'] = assets_df['closed'].apply(lambda x: x.value if not pd.isnull(x) else x)
    return assets_df


def _make_group_by_figi_from_df_by_period(assets_df: pd.DataFrame) -> pd.DataFrame:
    unique_figi = assets_df.figi.drop_duplicates()
    for figi in unique_figi:
        series_closed = assets_df[assets_df.figi == figi]['closed']

        assets_df.loc[assets_df.figi == figi, 'incr_rate'] = (
            series_closed / series_closed.shift()
        ).fillna(1)

        assets_df.loc[assets_df.figi == figi, 'total_incr'] = (
            assets_df.loc[assets_df.figi == figi, 'incr_rate']
            .cumprod()
            .tail(1)
            .reset_index(drop=True)
            [0]
        )

    grouped_by_figi_stats = assets_df.groupby(["figi", "name", "currency"], as_index=False).agg(
        std_incr_rate=('incr_rate', 'std'),
        total_incr=('total_incr', 'max'),
        count=('dt', 'count'),
    )
    return grouped_by_figi_stats


def _cut_period_for_year_intervals(date_from: datetime.datetime, date_to: datetime.datetime) -> List[datetime.datetime]:
    """Cut period from date_from to date_to on year intervals because api does not allow to
        load more than 1 year candles

    :param date_from: start period
    :param date_to: end period
    :return: list with dates splitted to year intervals from date_from to date_to
    """
    intervals = [date_from]
    for _ in range((date_to - date_from).days // YEAR_DAYS):
        intervals.append(intervals[-1] + datetime.timedelta(days=YEAR_DAYS))
    if intervals[-1] < date_to:
        intervals.append(date_to)
    return intervals
