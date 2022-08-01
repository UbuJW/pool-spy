import argparse
import json
import math
import os.path
from datetime import datetime, timedelta, timezone, time
from dateutil.relativedelta import relativedelta

import pandas as pd

from nicehash import private_api


def try_parsing_datetime(s):
    try:
        return datetime.strptime(s, '%Y-%m-%d-%H:%M:%S').replace(tzinfo=timezone.utc)
    except ValueError:
        t = datetime.strptime(s, '%H:%M:%S')
        return datetime.now(timezone.utc).replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--base_url', dest="base", help="Api base url", default="https://api2.nicehash.com")
    parser.add_argument('-l', '--label', dest="label", help="label")
    parser.add_argument('-o', '--organization_id', dest="org", help="Organization id", required=True)
    parser.add_argument('-k', '--key', dest="key", help="Api key", required=True)
    parser.add_argument('-s', '--secret', dest="secret", help="Secret for api key", required=True)
    # parser.add_argument('-r', '--rigs', dest='rigs', help="Additional rigs", nargs='+', default=[])
    parser.add_argument('-d', '--days', dest='days', help="Lookback in days", type=int, choices=range(1, 7), default=7)
    parser.add_argument('-e', '--end_datetime', dest='end_datetime',
                        help='End datetime or time in UTC: yyyy-mm-dd-HH:MM:SS or HH:MM:SS',
                        type=lambda s: try_parsing_datetime(s), default=datetime.now(timezone.utc))
    parser.add_argument('-m', '--monthly', dest='monthly', help="Monthly report", action='store_true')
    parser.add_argument('-di', '--discord-id', dest='discord_id', help='Discord ID')
    parser.add_argument('-dt', '--discord-token', dest='discord_token', help='Discord Token')
    parser.add_argument('-pm', '--publish_monthly', dest='publish_monthly', help="Publish monthly report",
                        action='store_true')
    parser.add_argument('-pd', '--publish_daily', dest='publish_daily', help="Publish daily report",
                        action='store_true')
    args = parser.parse_args()

    end_datetime = args.end_datetime.astimezone(timezone.utc)
    if args.monthly:
        start_datetime = datetime(end_datetime.year, end_datetime.month, 1, tzinfo=end_datetime.tzinfo)
        if end_datetime.day == 1 and end_datetime.time() == time(0, 0):
            start_datetime = start_datetime - relativedelta(months=1)
        nb_days = (end_datetime - start_datetime) / timedelta(microseconds=1) / 10 ** 6 / 60 / 60 / 24
    else:
        nb_days = args.days
        start_datetime = end_datetime - timedelta(days=nb_days)

    rigs_filepath = f'rigs_{args.org}_{start_datetime:%Y_%m}.json'
    if os.path.exists(rigs_filepath):
        with open(rigs_filepath, 'r') as fp:
            rig_ids_names = json.load(fp)
    else:
        rig_ids_names = {}
    private_api = private_api(args.base, args.org, args.key, args.secret)
    registered_rigs = {rig['rigId']: rig['name'] for rig in private_api.get_rigs()['miningRigs']}
    # rig_ids_names |= registered_rigs #| {rig: rig for rig in args.rigs} # for 3.9
    rig_ids_names = {**rig_ids_names, **registered_rigs}
    with open(rigs_filepath, 'w') as fp:
        json.dump(rig_ids_names, fp)

    start_timestamp = int(math.floor(start_datetime.astimezone().timestamp()) * 1000)
    end_timestamp = int(math.ceil(end_datetime.astimezone().timestamp()) * 1000)
    df_results = pd.DataFrame(columns=['hours/day', 'MH/s', '\u03BCBTC/day'])
    title = f'{args.label} {start_datetime:%B %Y}' if args.monthly else args.label
    if args.label is not None:
        print(title)
    dict_daily_hours = {}

    print(f'{start_datetime:%b %d %Y %H:%M:%S %Z} to {end_datetime:%b %d %Y %H:%M:%S %Z}')
    for rig_id, rig_name in rig_ids_names.items():
        stats = private_api.get_rig_stats(rig_id, start_timestamp, end_timestamp)
        df = pd.DataFrame.from_records(stats['data'], columns=stats['columns'], index='time').sort_index()
        if args.monthly:
            filename = f'{args.org}_{rig_id}_{start_datetime:%Y_%m}.csv'
            if os.path.exists(filename):
                df_cache = pd.read_csv(filename, index_col='time')
                if len(df) > 0:
                    df_cache = df_cache[df_cache.index < df.index[0]]
                df = pd.concat([df_cache, df], sort=True)
                assert not any(df.index.duplicated())
            df.reindex(sorted(df.columns), axis=1).to_csv(filename)
        df = df[['speed_accepted', 'profitability']]
        speed_diff = df.loc[:, 'speed_accepted'].diff().shift(-1)
        start_times = df.index.values[:-1]
        end_times = df.index.values[1:]
        df = df.iloc[:-1]
        df['speed_diff'] = speed_diff
        df['speed_diff'] = df['speed_diff'].fillna(0)
        df['time_delta'] = end_times - start_times if any(end_times) else 0
        df.loc[df['time_delta'] > 5 * 60 * 1000, 'speed_diff'] = 0
        df.index = pd.to_datetime(df.index, unit='ms', utc=True)
        df = df[df['speed_diff'] != 0]
        dict_daily_hours[rig_name] = df.groupby(df.index.date).sum()['time_delta'] / 1000 / 60 / 60
        total_mining_ms = df['time_delta'].sum()
        avg_hr_per_day = total_mining_ms / 1000 / 60 / 60 / nb_days
        mh_per_sec = df[['speed_accepted', 'time_delta']].prod(axis=1).sum() / 1000 / 60 / 60 / 24 / nb_days
        profitability = df[['profitability', 'time_delta']].prod(axis=1).sum() / 1000 / 60 / 60 / 24 / nb_days
        df_results = pd.concat([df_results, pd.DataFrame([{'hours/day': avg_hr_per_day, 'MH/s': mh_per_sec,
                                                           '\u03BCBTC/day': profitability * 10 ** 6}],
                                                         columns=df_results.columns, index=[rig_name])])
    df_daily_hours = pd.concat(dict_daily_hours, axis=1, sort=True).fillna(0)
    df_daily_hours.to_csv(f'daily_hours_{args.org}_{start_datetime:%Y_%m}.csv')
    import matplotlib.dates as md
    import matplotlib.pyplot as plt

    plt.gca().xaxis.set_major_formatter(md.DateFormatter('%H:%M'))
    fig = df_daily_hours.expanding().mean().plot(title=f'{title} cumulative average hours',
                                                 yticks=range(0, 25, 2)).get_figure()
    plt.axhline(y=8, color='r', linestyle='--')
    plt.gca().xaxis.set_major_formatter(md.DateFormatter('%d'))
    fig.savefig(f'daily_hours_{args.org}_{start_datetime:%Y_%m}.png')
    df_results = df_results.sort_index()
    df_results.loc["Total"] = df_results.sum()
    df_results.index.name = 'rig'
    # df_results.to_string(formatters={'hours/day': '{:,.2f}'.format, 'MH/s': '{:,.2f}'.format,
    #                                    '\u03BCBTC/day': '{:,.2f}'.format})
    df_results.to_markdown(floatfmt='.2f', tablefmt='github')
    lines = df_results.to_markdown(floatfmt='.2f', tablefmt='github').splitlines()
    lines.insert(-1, lines[1])
    lines[1] = lines[1].replace('-', '=')
    results_str = os.linesep.join(lines)
    print(results_str)

    if args.discord_id is None or args.discord_token is None:
        exit(0)
    from discord import Webhook, RequestsWebhookAdapter, Embed, File

    webhook = Webhook.partial(args.discord_id, args.discord_token, adapter=RequestsWebhookAdapter())
    embed = Embed()
    embed.title = title
    embed.colour = 15258703
    embed.description = f'```{start_datetime:%b %d %Y %H:%M:%S %Z} to {end_datetime:%b %d %Y %H:%M:%S %Z}\n' \
                        f'{results_str}```'
    if args.publish_monthly:
        print('Publish monthly report')
        webhook.send(username='Earn Your Hours', embed=embed)
    if args.publish_daily:
        print('Publish daily report')
        with open(file=f'daily_hours_{args.org}_{start_datetime:%Y_%m}.csv', mode='rb') as f:
            daily_hours_file = File(f)
        with open(file=f'daily_hours_{args.org}_{start_datetime:%Y_%m}.png', mode='rb') as f:
            daily_hours_fig = File(f)
        webhook.send(username='Earn Your Hours', content=embed.title, files=[daily_hours_file, daily_hours_fig])
    exit(0)
