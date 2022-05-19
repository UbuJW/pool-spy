import argparse
import math
from datetime import datetime, timedelta, timezone, time

import pandas as pd

from nicehash import private_api


def try_parsing_datetime(s):
    try:
        return datetime.strptime(s, '%Y-%m-%d-%H:%M:%S').replace(tzinfo=timezone.utc)
    except ValueError:
        t = datetime.strptime(s, '%H:%M:%S')
        return datetime.now(timezone.utc).replace(hour=t.hour, minute=t.minute, second=t.second)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-b', '--base_url', dest="base", help="Api base url", default="https://api2.nicehash.com")
    parser.add_argument('-o', '--organization_id', dest="org", help="Organization id", required=True)
    parser.add_argument('-k', '--key', dest="key", help="Api key", required=True)
    parser.add_argument('-s', '--secret', dest="secret", help="Secret for api key", required=True)
    parser.add_argument('-r', '--rigs', dest='rigs', help="Additional rigs", nargs='+', default=[])
    parser.add_argument('-d', '--days', dest='days', help="Lookback in days", type=int, choices=range(1, 7), default=7)
    parser.add_argument('-e', '--end_datetime', dest='end_datetime', help='End datetime in UTC: yyyy-mm-dd-HH:MM:SS',
                        type=lambda s: try_parsing_datetime(s), default=datetime.now(timezone.utc))

    args = parser.parse_args()
    private_api = private_api(args.base, args.org, args.key, args.secret)
    rigs = private_api.get_rigs()
    rig_ids_names = {rig['rigId']: rig['name'] for rig in rigs['miningRigs']}
    for rig in args.rigs:
        rig_ids_names[rig] = rig
    end_datetime = args.end_datetime.astimezone(timezone.utc)
    nb_days = args.days
    start_datetime = end_datetime - timedelta(days=nb_days)
    start_timestamp = int(math.floor(start_datetime.astimezone().timestamp())*1000)
    end_timestamp = int(math.ceil(end_datetime.astimezone().timestamp())*1000)
    df_results = pd.DataFrame(columns=['hours/day', 'MH/s', 'BTC/day'])
    print(f'{start_datetime:%b %d %Y %H:%M:%S %Z} to {end_datetime:%b %d %Y %H:%M:%S %Z}')
    for rig_id, rig_name in rig_ids_names.items():
        stats = private_api.get_rig_stats(rig_id, start_timestamp, end_timestamp)
        df = pd.DataFrame.from_records(stats['data'], columns=stats['columns'], index='time').sort_index()
        df = df[['speed_accepted', 'profitability']]
        speed_diff = df['speed_accepted'].diff()
        start_times = df.index.values[:-1]
        end_times = df.index.values[1:]
        df = df.iloc[:-1]
        df['speed_diff'] = speed_diff[1:]
        df['speed_diff'] = df['speed_diff'].fillna(0)
        df['time_delta'] = end_times - start_times
        df.index = pd.to_datetime(df.index, unit='ms', utc=True)
        df = df[df['speed_diff'] != 0]
        total_ms = df['time_delta'].sum()
        avg_hr_per_day = total_ms / 1000 / 60 / 60 / nb_days
        mh_per_sec = df[['speed_accepted', 'time_delta']].prod(axis=1).sum() / total_ms if total_ms != 0 else 0
        profitability = df[['profitability', 'time_delta']].prod(axis=1).sum() / 1000 / 60 / 60 / 24 / nb_days
        df_results = df_results.append(pd.DataFrame([{'hours/day': avg_hr_per_day,
                                                      'MH/s': mh_per_sec, 'BTC/day': profitability}],
                                                    columns=df_results.columns, index=[rig_name]))
    print(df_results.sort_index().to_string(formatters={'hours/day': '{:,.2f}'.format, 'MH/s': '{:,.2f}'.format,
                                                        'BTC/day': '{:,.8f}'.format}))

    exit(0)
