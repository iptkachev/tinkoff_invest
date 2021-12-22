import datetime
import time
from collections import namedtuple
from typing import List
import pandas as pd
from tqdm import tqdm
import tinvest

Asset = namedtuple("Asset", ["name", "figi", "currency", "closed"])
ts_value = namedtuple("TsValue", ["dttm", "value"])


def get_stats_for_period(client: tinvest.SyncClient,
                         market_instruments: tinvest.MarketInstrumentListResponse,
                         date_from: datetime.datetime, date_to: datetime.datetime,
                         sleep_seconds=0) -> pd.DataFrame:
    assets = list(map(lambda inst: Asset(inst.name, inst.figi, inst.currency.name, []),
                      market_instruments.payload.instruments))

    for i, a in tqdm(enumerate(assets), total=len(assets)):
        a.closed.extend(
            list(map(
                lambda x: ts_value(x.time.date(), float(x.c)),
                client.get_market_candles(
                    a.figi,
                    from_=date_from,
                    to=date_to,
                    interval=tinvest.schemas.CandleResolution.day
                ).payload.candles
            ))
        )

        if i % 90 == 0 and i >= 90:
            time.sleep(sleep_seconds)

    grouped_by_figi_stats = _make_df_from_assets(assets)
    return grouped_by_figi_stats


def _get_top_std_top_increase(grouped_by_figi_stats: pd.DataFrame,
                              currency: str,
                              top_std: int,
                              ascending_std: bool,
                              top_increase: int,
                              ascending_increase: bool) -> pd.DataFrame:
    return (
        grouped_by_figi_stats[grouped_by_figi_stats.currency == currency]
        .sort_values('std_increase_rate', ascending=ascending_std)
        .head(top_std)
        .sort_values('total_increase', ascending=ascending_increase)
        .head(top_increase)
    )


def _get_top_increase(grouped_by_figi_stats: pd.DataFrame,
                      currency: str,
                      top_increase: int,
                      ascending_increase: bool) -> pd.DataFrame:
    return (
        grouped_by_figi_stats[grouped_by_figi_stats.currency == currency]
        .sort_values('total_increase', ascending=ascending_increase)
        .head(top_increase)
    )


def _make_df_from_assets(assets: List[Asset]) -> pd.DataFrame:
    assets_df = pd.DataFrame(assets).dropna()
    assets_df = assets_df[assets_df.closed.apply(len) > 1]
    assets_df = assets_df.explode('closed')
    assets_df = assets_df.reset_index(drop=True)

    assets_df['dt'] = assets_df['closed'].apply(lambda x: x.dttm if not pd.isnull(x) else x)
    assets_df['closed'] = assets_df['closed'].apply(lambda x: x.value if not pd.isnull(x) else x)

    unique_figi = assets_df.figi.drop_duplicates()
    for figi in unique_figi:
        series_closed = assets_df[assets_df.figi == figi]['closed']

        assets_df.loc[assets_df.figi == figi, 'increase_rate'] = (
            series_closed / series_closed.shift()
        ).fillna(1)

        assets_df.loc[assets_df.figi == figi, 'total_increase'] = (
            assets_df.loc[assets_df.figi == figi, 'increase_rate']
            .cumprod()
            .tail(1)
            .reset_index(drop=True)
            [0]
        )

    grouped_by_figi_stats = assets_df.groupby(["figi", "name", "currency"], as_index=False).agg(
        std_increase_rate=('increase_rate', 'std'),
        total_increase=('total_increase', 'max'),
        count=('dt', 'count'),
    )
    return grouped_by_figi_stats
