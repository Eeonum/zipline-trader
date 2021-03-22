import sys
# sys.path.insert(0, 'C:/Users/rapha/OneDrive/Dokumente/GitHub/data/python_code')

# PROJECT'S PACAKGES
import pck_param as param
import pck_data_load as data_load
import pck_interface as interface

import pandas as pd
import numpy as np
# from pathlib import Path
from six import iteritems
from logbook import Logger

# from zipfile import ZipFile
show_progress = True
log = Logger(__name__)


# FORMAT THE DATA --> impport data from a zipfile and set the correct columns' names
def load_data_table(show_progress=show_progress):
    db_info, data_source_info = param.parse_credentials()

    sql_conn, sql_cursor = data_load.mysql_connect(
        host=db_info['host'],
        port=db_info['port'],
        user=db_info['user'],
        password=db_info['password'],
        database=db_info['database'])

    data = interface.SQL_to_python_data_loader(sql_conn=sql_conn,
                                               sql_cursor=sql_cursor,
                                               security_symbol_1='IBM',
                                               security_symbol_3='AACG',
                                               perioodicityID='D')

    if show_progress:
        log.info(data.info())
        log.info(data.head())

    return data


# CREATE THE METADATA --> The metadata table provides the �mapping table� for our securities list
def gen_asset_metadata(data, show_progress):
    if show_progress:
        log.info('Generating asset metadata.')
    data = data.groupby(
        by='symbol'
    ).agg(
        {'date': [np.min, np.max]}
    )
    data.reset_index(inplace=True)
    data['start_date'] = data.date.amin
    data['end_date'] = data.date.amax
    # RC: add exchange
    data['exchange'] = 'NYSE'
    del data['date']
    data.columns = data.columns.get_level_values(0)

    data['auto_close_date'] = data['end_date'].values
    if show_progress:
        log.info(data.info())
        log.info(data.head())
    return data


# STORE THE ADJUSTMENTS
def parse_splits(data, show_progress):
    if show_progress:
        log.info('Parsing split data.')
    data['split_ratio'] = 1.0 / data.split_ratio
    data.rename(
        columns={
            'split_ratio': 'ratio',
            'date': 'effective_date',
        },
        inplace=True,
        copy=False,
    )
    if show_progress:
        log.info(data.info())
        log.info(data.head())
    return data


def parse_dividends(data, show_progress):
    if show_progress:
        log.info('Parsing dividend data.')
    data['record_date'] = data['declared_date'] = data['pay_date'] = pd.NaT
    data.rename(columns={'date': 'ex_date',
                         'dividends': 'amount'}, inplace=True, copy=False)
    if show_progress:
        log.info(data.info())
        log.info(data.head())
    return data


# WRITE THE DAILY BARS
def parse_pricing_and_vol(data,
                          sessions,
                          symbol_map):
    for asset_id, symbol in iteritems(symbol_map):
        asset_data = data.xs(
            symbol,
            level=1
        ).reindex(
            sessions.tz_localize(None)
        ).fillna(0.0)
        yield asset_id, asset_data


def ingest(environ,
           asset_db_writer,
           minute_bar_writer,
           daily_bar_writer,
           adjustment_writer,
           calendar,
           start_session,
           end_session,
           cache,
           show_progress,
           output_dir):
    raw_data = load_data_table(show_progress=show_progress)
    # raw_data = load_data_table(path, show_progress=show_progress)
    asset_metadata = gen_asset_metadata(
        raw_data[['symbol', 'date']],
        show_progress
    )

    exchange = {'exchange': 'NYSE', 'canonical_name': 'NYSE', 'country_code': 'US'}
    exchange_df = pd.DataFrame(exchange, index=[0])
    asset_db_writer.write(equities=asset_metadata, exchanges=exchange_df)

    # WRITE THE DAILY BARS
    symbol_map = asset_metadata.symbol
    sessions = calendar.sessions_in_range(start_session, end_session)
    raw_data.set_index(['date', 'symbol'], inplace=True)
    daily_bar_writer.write(
        parse_pricing_and_vol(
            raw_data,
            sessions,
            symbol_map
        ),
        show_progress=show_progress
    )
    # STORE THE ADJUSTMENTS
    raw_data.reset_index(inplace=True)
    raw_data['symbol'] = raw_data['symbol'].astype('category')
    raw_data['sid'] = raw_data.symbol.cat.codes
    adjustment_writer.write(
        splits=parse_splits(
            raw_data[[
                'sid',
                'date',
                'split_ratio',
            ]].loc[raw_data.split_ratio != 1],
            show_progress=show_progress
        ),
        dividends=parse_dividends(
            raw_data[[
                'sid',
                'date',
                'dividends',
            ]].loc[raw_data.dividends != 0],
            show_progress=show_progress
        )
    )


def csv_to_bundle(interval='1m'):
    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir
               ):

        # Get all available csv filenames
        csv_filenames = save_csv(reload_tickers=True, interval=interval)
        # Loop through the filenames and create a dict to keep some temp meta data
        ticker_pairs = [{'exchange': pair.split('_')[0],
                         'symbol': pair.split('_')[1],
                         'interval': pair.split('_')[2].split('.')[0],
                         'file_path': join(csv_data_path, pair)}
                        for pair in csv_filenames]

        # Create an empty meta data dataframe
        metadata_dtype = [
            ('symbol', 'object'),
            ('asset_name', 'object'),
            ('start_date', 'datetime64[ns]'),
            ('end_date', 'datetime64[ns]'),
            ('first_traded', 'datetime64[ns]'),
            ('auto_close_date', 'datetime64[ns]'),
            ('exchange', 'object'), ]
        metadata = pd.DataFrame(
            np.empty(len(ticker_pairs), dtype=metadata_dtype))

        minute_data_sets = []
        daily_data_sets = []

        for sid, ticker_pair in enumerate(ticker_pairs):
            df = pd.read_csv(ticker_pair['file_path'],
                             index_col=['date'],
                             parse_dates=['date'])

            symbol = ticker_pair['symbol']
            asset_name = ticker_pair['symbol']
            start_date = df.index[0]
            end_date = df.index[-1]
            first_traded = start_date
            auto_close_date = end_date + pd.Timedelta(days=1)
            exchange = ticker_pair['exchange']

            # Update metadata
            metadata.iloc[sid] = symbol, asset_name, start_date, end_date, first_traded, auto_close_date, exchange

            if ticker_pair['interval'] == '1m':
                minute_data_sets.append((sid, df))

            if ticker_pair['interval'] == '1d':
                daily_data_sets.append((sid, df))

        if minute_data_sets != []:
            # Dealing with missing sessions in some data sets
            for daily_data_set in daily_data_sets:
                try:
                    minute_bar_writer.write(
                        [daily_data_set], show_progress=True)
                except Exception as e:
                    print(e)

        if daily_data_sets != []:
            # Dealing with missing sessions in some data sets
            for daily_data_set in daily_data_sets:
                try:
                    daily_bar_writer.write(
                        [daily_data_set], show_progress=True)
                except Exception as e:
                    print(e)

        asset_db_writer.write(equities=metadata)
        print(metadata)
        adjustment_writer.write()

    return ingest