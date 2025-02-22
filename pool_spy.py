import argparse
import json
import math
import os.path
from datetime import datetime, timedelta, timezone, time

import pandas as pd
from dateutil.relativedelta import relativedelta

from nicehash import private_api, AlgorithmType

MINING_HOURS_THRESHOLD = 8

ALGOS = {algo for algo in AlgorithmType}


def try_parsing_datetime(s):
    try:
        return datetime.strptime(s, '%Y-%m-%d-%H:%M:%S').replace(tzinfo=timezone.utc)
    except ValueError:
        t = datetime.strptime(s, '%H:%M:%S')
        return datetime.now(timezone.utc).replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=0)


def ts_dict_to_df(dico):
    return pd.DataFrame.from_records(dico['data'], columns=dico['columns'], index='time').sort_index()


def merge_and_cache_timeseries(df: pd.DataFrame, filepath: str, monthly: bool = False):
    if monthly:
        if os.path.exists(filepath):
            df_cache = pd.read_csv(filepath, index_col='time')
            if len(df) > 0:
                df_cache = df_cache[df_cache.index < df.index[0]]
            df = pd.concat([df_cache, df], sort=True)
            assert not any(df.index.duplicated())
        df.reindex(sorted(df.columns), axis=1).to_csv(filepath)
    return df


def save_fig(df: pd.DataFrame):
    from matplotlib import dates as md, pyplot as plt
    plt.gca().xaxis.set_major_formatter(md.DateFormatter('%H:%M'))
    fig = df.expanding().mean().plot(title=f'{title} cumulative average hours',
                                     yticks=range(0, 25, 2)).get_figure()
    plt.axhline(y=MINING_HOURS_THRESHOLD, color='r', linestyle='--')
    plt.gca().xaxis.set_major_formatter(md.DateFormatter('%d'))
    fig.savefig(os.path.join('data', f'daily_hours_{args.org}_{start_datetime:%Y_%m}.png'))


def get_btcusd():
    import requests
    response = requests.get('https://api.coindesk.com/v1/bpi/currentprice.json')
    data = response.json()
    return float(data["bpi"]["USD"]["rate"].replace(',', ''))


def try_load_json(filepath: str):
    if os.path.exists(filepath):
        with open(filepath, 'r') as fp:
            dico = json.load(fp)
    else:
        dico = {}
    return dico


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--base_url', dest="base", help="Api base url", default="https://api2.nicehash.com")
    parser.add_argument('-l', '--label', dest="label", help="label")
    parser.add_argument('-o', '--organization_id', dest="org", help="Organization id", required=True)
    parser.add_argument('-k', '--key', dest="key", help="Api key", required=True)
    parser.add_argument('-s', '--secret', dest="secret", help="Secret for api key", required=True)
    parser.add_argument('-r', '--rigs', dest='rigs', help="Additional rigs", nargs='+', default=[])
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
    parser.add_argument('-p', '--payout', dest='payout', help="Payout", action='store_true')
    args = parser.parse_args()

    if not os.path.exists('data'):
        os.makedirs('data')

    end_datetime = args.end_datetime.astimezone(timezone.utc)
    if args.monthly:
        start_datetime = datetime(end_datetime.year, end_datetime.month, 1, tzinfo=end_datetime.tzinfo)
        if end_datetime.day == 1 and end_datetime.time() == time(0, 0):
            start_datetime = start_datetime - relativedelta(months=1)
        nb_days = (end_datetime - start_datetime) / timedelta(microseconds=1) / 10 ** 6 / 60 / 60 / 24
    else:
        nb_days = args.days
        start_datetime = end_datetime - timedelta(days=nb_days)

    rigs_filepath = os.path.join('data', f'rigs_{args.org}_{start_datetime:%Y_%m}.json')
    rig_ids_names = try_load_json(rigs_filepath)
    private_api = private_api(args.base, args.org, args.key, args.secret)
    registered_rigs = {rig['rigId']: rig['name'] for rig in private_api.get_rigs()['miningRigs']}
    # rig_ids_names |= registered_rigs | {rig: rig for rig in args.rigs} # for 3.9
    rig_ids_names = {**rig_ids_names, **registered_rigs, **{rig: rig for rig in args.rigs}}
    with open(rigs_filepath, 'w') as rigs_file:
        json.dump(rig_ids_names, rigs_file)

    start_timestamp = int(math.floor(start_datetime.astimezone().timestamp()) * 1000)
    end_timestamp = int(math.ceil(end_datetime.astimezone().timestamp()) * 1000)
    df_results = pd.DataFrame(columns=['hours/day', 'MH/s', '\u03BCBTC/day'])
    title = f'{args.label} {start_datetime:%B %Y}' if args.monthly else args.label
    if args.label is not None:
        print(title)
    list_daily_hours = []

    print(f'{start_datetime:%b %d %Y %H:%M:%S %Z} to {end_datetime:%b %d %Y %H:%M:%S %Z}')
    df_ts = pd.concat([ts_dict_to_df(private_api.get_pool_stats(start_timestamp, end_timestamp, algo))
                       for algo in ALGOS], axis=0)
    df_ts = df_ts.groupby(df_ts.index).agg('sum').sort_index()
    filename = os.path.join('data', f'{args.org}.csv')
    df_ts = merge_and_cache_timeseries(df_ts, filename, args.monthly)
    for rig_id, rig_name in rig_ids_names.items():
        df_ts = pd.concat([ts_dict_to_df(private_api.get_rig_stats(rig_id, start_timestamp, end_timestamp, algo))
                           for algo in ALGOS], axis=0)
        df_ts = df_ts.groupby(df_ts.index).agg('sum').sort_index()
        filename = os.path.join('data', f'{args.org}_{rig_id}_{start_datetime:%Y_%m}.csv')
        df_ts = merge_and_cache_timeseries(df_ts, filename, args.monthly)
        df_ts = df_ts[['speed_accepted', 'profitability']]
        speed_diff = df_ts.loc[:, 'speed_accepted'].diff().shift(-1)
        start_times = df_ts.index.values[:-1]
        end_times = df_ts.index.values[1:]
        df_ts = df_ts.iloc[:-1]
        df_ts['speed_diff'] = speed_diff
        df_ts['speed_diff'] = df_ts['speed_diff'].fillna(0)
        df_ts['time_delta'] = end_times - start_times if any(end_times) else 0
        df_ts.loc[df_ts['time_delta'] > 5 * 60 * 1000, 'speed_diff'] = 0
        df_ts.index = pd.to_datetime(df_ts.index, unit='ms', utc=True)
        df_ts = df_ts[df_ts['speed_diff'] != 0]
        df_daily_hours = df_ts.groupby(df_ts.index.date).sum()['time_delta'] / 1000 / 60 / 60
        df_daily_hours.name = rig_name
        list_daily_hours.append(df_daily_hours)
        total_mining_ms = df_ts['time_delta'].sum()
        avg_hr_per_day = total_mining_ms / 1000 / 60 / 60 / nb_days
        mh_per_sec = df_ts[['speed_accepted', 'time_delta']].prod(axis=1).sum() / 1000 / 60 / 60 / 24 / nb_days
        profitability = df_ts[['profitability', 'time_delta']].prod(axis=1).sum() / 1000 / 60 / 60 / 24 / nb_days
        df_results = pd.concat([df_results, pd.DataFrame([{'hours/day': avg_hr_per_day, 'MH/s': mh_per_sec,
                                                           '\u03BCBTC/day': profitability * 10 ** 6}],
                                                         columns=df_results.columns, index=[rig_name])])
    df_daily_hours = pd.concat(list_daily_hours, axis=1, sort=True).fillna(0).groupby(level=0, axis=1, sort=True).sum()
    df_daily_hours.to_csv(os.path.join('data', f'daily_hours_{args.org}_{start_datetime:%Y_%m}.csv'))
    df_daily_hours.index = pd.to_datetime(df_daily_hours.index, format='%Y-%m-%d').strftime('%d')
    print('Daily mining hours')
    print(df_daily_hours.to_markdown(floatfmt='.2f', tablefmt='github'))
    save_fig(df_daily_hours)

    df_results.index.name = 'rig'
    df_results = df_results.reset_index().groupby('rig', sort=True).sum()
    df_results.loc["Total"] = df_results.sum()
    lines = df_results.to_markdown(floatfmt='.2f', tablefmt='github').splitlines()
    lines.insert(-1, lines[1])
    lines[1] = lines[1].replace('-', '=')
    results_str = os.linesep.join(lines)
    print('\nMonthly mining stats')
    print(results_str)

    if args.discord_id is not None and args.discord_token is not None:
        from discord import Webhook, RequestsWebhookAdapter, Embed, File

        webhook = Webhook.partial(args.discord_id, args.discord_token, adapter=RequestsWebhookAdapter())
        embed = Embed()
        embed.title = title
        embed.colour = 15258703
        embed.description = f'```{start_datetime:%b %d %Y %H:%M:%S %Z} to {end_datetime:%b %d %Y %H:%M:%S %Z}\n' \
                            f'{results_str}```'
        if args.publish_monthly:
            print('\nPublish monthly report')
            webhook.send(username='Earn Your Hours', embed=embed)
        if args.publish_daily:
            print('\nPublish daily report')
            with open(file=os.path.join('data', f'daily_hours_{args.org}_{start_datetime:%Y_%m}.csv'), mode='rb') as f:
                daily_hours_file = File(f)
            with open(file=os.path.join('data', f'daily_hours_{args.org}_{start_datetime:%Y_%m}.png'), mode='rb') as f:
                daily_hours_fig = File(f)
            webhook.send(username='Earn Your Hours', content=embed.title, files=[daily_hours_file, daily_hours_fig])

    if args.payout:
        print('\nPayout')
        addresses_filepath = os.path.join('data', f'addresses_{args.label}.json')
        dict_addresses = try_load_json(addresses_filepath)
        dict_addresses = {**dict_addresses, **{address['name']: {'id': address['id'], 'address': address['address']}
                                               for address in private_api.get_withdrawal_addresses('BTC')['list']}}
        threshold_rig_names = df_results[:-1][df_results[:-1]['hours/day'] >= MINING_HOURS_THRESHOLD].index.tolist()
        if not any(threshold_rig_names):
            print(f'No miner mined for at least {MINING_HOURS_THRESHOLD} hours per day.')
            exit(0)
        available_btc = float(private_api.get_accounts_for_currency('BTC')['available'])
        str_rig_names = threshold_rig_names[0] if len(threshold_rig_names) == 1 else ', '.join(
            threshold_rig_names[:-1]) + f' and {threshold_rig_names[-1]}'
        print(f'{str_rig_names} mined for at least {MINING_HOURS_THRESHOLD} hours per day.')
        # use floor division to ensure payout is rounded down
        payout = available_btc * 1e8 // len(threshold_rig_names) * 1e-8
        btcusd = get_btcusd()
        print(f'{available_btc / 1e-6:.2f} \u03BCBTC (${btcusd * available_btc:.2f}) available in {args.label} wallet.')
        print(f'Sending {payout / 1e-6:.2f} \u03BCBTC (${btcusd * payout:.2f}) '
              f'less 5 \u03BCBTC (${btcusd * 5 * 1e-6:.2f}) NH withdrawal fee to:')
        for rig_name in threshold_rig_names:
            dict_address = dict_addresses[rig_name]
            try:
                id = private_api.withdraw_request(dict_address['id'], payout, 'BTC')
                print(f'\t{rig_name}: {dict_address["address"]}, id: {id}')
                # private_api.cancel_withdraw_request(id, 'BTC')
            except BaseException as err:
                print(err)
    exit(0)
