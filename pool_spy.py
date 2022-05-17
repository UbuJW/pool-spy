import argparse
from datetime import datetime, timedelta, timezone

import pandas as pd

from nicehash import private_api

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-b', '--base_url', dest="base", help="Api base url", default="https://api2.nicehash.com")
    parser.add_argument('-o', '--organization_id', dest="org", help="Organization id", required=True)
    parser.add_argument('-k', '--key', dest="key", help="Api key", required=True)
    parser.add_argument('-s', '--secret', dest="secret", help="Secret for api key", required=True)
    parser.add_argument('-r', '--rigs', dest='rigs', help="Additional rigs", nargs='+', default=[])

    args = parser.parse_args()
    private_api = private_api(args.base, args.org, args.key, args.secret)
    rigs = private_api.get_rigs()
    rig_ids_names = {rig['rigId']: rig['name'] for rig in rigs['miningRigs']}
    for rig in args.rigs:
        rig_ids_names[rig] = rig
    now = datetime.now(timezone.utc)
    nb_days = 7
    start_time = now - timedelta(days=nb_days)
    start_time_ms = private_api.get_epoch_ms(start_time)
    end_time_ms = private_api.get_epoch_ms(now)
    df_results = pd.DataFrame(columns=['hours/day', 'MH/s', 'BTC/day'])
    print(f'{start_time:%b %d %Y %H:%M:%S} UTC to {now:%b %d %Y %H:%M:%S} UTC')
    for rig_id, rig_name in rig_ids_names.items():
        stats = private_api.get_rig_stats(rig_id, start_time_ms, end_time_ms)
        df = pd.DataFrame.from_records(stats['data'], columns=stats['columns'], index='time').sort_index()
        speed_diff = df['speed_accepted'].diff()
        start_times = df.index.values[:-1]
        end_times = df.index.values[1:]
        df = df.iloc[:-1]
        df['speed_diff'] = speed_diff[1:]
        df['time_delta'] = end_times - start_times
        df = df[df['speed_diff'] != 0]
        avg_hr_per_day = df['time_delta'].sum() / 1000 / 60 / 60 / nb_days
        mh_per_sec = df[['speed_accepted', 'time_delta']].prod(axis=1).sum() / 1000 / 60 / 60 / 24 / nb_days
        profitability = df[['profitability', 'time_delta']].prod(axis=1).sum() / 1000 / 60 / 60 / 24 / nb_days
        df_results = df_results.append(pd.DataFrame([{'hours/day': avg_hr_per_day,
                                                      'MH/s': mh_per_sec, 'BTC/day': profitability}],
                                                    columns=df_results.columns, index=[rig_name]))
    print(df_results.sort_index().to_string(formatters={'hours/day': '{:,.2f}'.format, 'MH/s': '{:,.2f}'.format,
                                                        'BTC/day': '{:,.8f}'.format}))

    exit(0)
