import json
import os
import gspread
import pandas as pd
import time
from colorama import Fore, init
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from datetime import datetime
from gspread_dataframe import set_with_dataframe
import aiohttp
import asyncio

init(autoreset=True)
load_dotenv()

start_time = time.time()

async def get_date_range():
    start_date = datetime(2024, 4, 1)
    end_date = datetime.now()
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    return json.dumps({'since': start_date_str, 'until': end_date_str})

async def google_sheet_import(fb_ads_data, sheet_name):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('fb-ads-data-423818-c13708cd5253.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open('Meta_Ads_data')

    try:
        worksheet = sheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=sheet_name, rows=2000, cols=20)
    try:
        set_with_dataframe(worksheet, fb_ads_data, include_column_header=True, resize=True)
        print(f"\n{Fore.LIGHTRED_EX}Imported data to Google Sheets ✅\n")
    except gspread.exceptions.APIError as e:
        print(f"Failed to update Google Sheets due to an API error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def extract_action(action_list, action_type):
    for action in action_list:
        if action['action_type'] == action_type:
            return action['value']
    return 0

async def fetch_meta(access_token, account_id):
    data = []
    time_range = await get_date_range()

    url = f'https://graph.facebook.com/v20.0/act_{account_id}/insights'
    params = {
        'access_token': access_token,
        'time_range': time_range,
        'fields': 'account_currency, campaign_id, campaign_name, impressions, clicks, spend, reach, actions, objective',
        'time_increment': 1,
        'level': 'campaign'
    }

    async with aiohttp.ClientSession() as session:
        while url:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    response_json = await response.json()
                    data.extend(response_json['data'])
                    url = response_json.get('paging', {}).get('next')
                else:
                    print(f"Error: {response.status}")
                    print(await response.text())
                    break

    fb_ads_data = pd.DataFrame(data)

    columns = ['date_start', 'account_currency', 'campaign_name', 'campaign_id', 'impressions', 'clicks', 'spend', 'reach', 'actions', 'objective']
    for column in columns:
        if column not in fb_ads_data.columns:
            fb_ads_data[column] = None

    action_columns = ['link_clicks', 'video_views', 'post_engagements', 'post_reactions']
    action_types = ['link_click', 'video_view', 'post_engagement', 'post_reaction']
    for action_col, action_type in zip(action_columns, action_types):
        fb_ads_data[action_col] = fb_ads_data['actions'].apply(lambda x: extract_action(x, action_type) if isinstance(x, list) else 0)

    fb_ads_data = fb_ads_data[['date_start', 'account_currency', 'campaign_id', 'campaign_name', 'impressions', 'clicks', 'spend', 'reach', 'link_clicks', 'video_views', 'post_engagements', 'post_reactions', 'objective']]

    return fb_ads_data

async def fetch_adset_ad(session, access_token, account_id, level, id, id2, name):
    data = []
    time_range = await get_date_range()

    url = f'https://graph.facebook.com/v20.0/act_{account_id}/insights'
    params = {
        'access_token': access_token,
        'time_range': time_range,
        'fields': f"{id}, {id2} ,{name}",
        'time_increment': 1,
        'level': level
    }

    while url:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                response_json = await response.json()
                data.extend(response_json['data'])
                url = response_json.get('paging', {}).get('next')
            else:
                print(f"Error: {response.status}")
                print(await response.text())
                break

    df = pd.DataFrame(data)
    return df

async def merge_data(adset_data, ad_data, df):
    df1 = pd.merge(adset_data, ad_data, on=['adset_id', 'date_start', 'date_stop'], how='outer')
    df2 = df1[['campaign_id', 'adset_name', 'ad_name', 'date_start']]
    data = pd.merge(df, df2, on=['campaign_id', 'date_start'], how='outer')
    final_data = data[['date_start', 'account_currency', 'campaign_name', 'adset_name', 'ad_name', 'impressions', 'clicks', 'spend', 'reach', 'link_clicks', 'video_views', 'post_engagements', 'post_reactions', 'objective']]
    final_data = final_data.drop_duplicates(subset='impressions')
    final_data = final_data.sort_values(by='date_start', ascending=False)
    final_data.to_csv('test.csv')
    return final_data

async def main():
    access_token = os.getenv("fb_access_token")
    account_ids = [os.getenv('account_id_1'), os.getenv('account_id_2'), os.getenv('account_id_3'), os.getenv('account_id_4')]

    async with aiohttp.ClientSession() as session:
        for account_id in account_ids:
            df = await fetch_meta(access_token=access_token, account_id=account_id)
            adset_data = await fetch_adset_ad(session, access_token, account_id, level='adset', id='adset_id', id2='campaign_id', name='adset_name')
            ad_data = await fetch_adset_ad(session, access_token, account_id, level='ad', id='ad_id', id2='adset_id', name='ad_name')

            data = await merge_data(adset_data=adset_data, ad_data=ad_data, df=df)
            print(data)
            sheet_name = f"meta_{account_id}"
            await google_sheet_import(data, sheet_name)

if __name__ == '__main__':
    asyncio.run(main())
