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
import requests


init(autoreset=True)
load_dotenv()

start_time = time.time()

start_date = datetime(2024, 5, 1)
end_date = datetime.now()
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')
time_range = json.dumps({'since': start_date_str, 'until': end_date_str})

def google_sheet_import(fb_ads_data, sheet_name):
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

def fetch_meta_insight_data(access_token, account_id):
    data = []
    start_date = datetime(2024, 1, 1)
    end_date = datetime.now()
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    time_range = json.dumps({'since': start_date_str, 'until': end_date_str})

    url = f'https://graph.facebook.com/v20.0/act_{account_id}/insights'
    params = {
        'access_token': access_token,
        'time_range': time_range,
        'fields': 'account_currency, campaign_name, adset_name, ad_name, impressions, clicks, spend, reach, actions, objective, outbound_clicks',
        'time_increment': 1,
        'level': {'ad', 'adset', 'campaign', 'account'}        
    }

    while url:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            response_json = response.json()
            data.extend(response_json['data'])
            url = response_json.get('paging', {}).get('next')
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            break

    fb_ads_data = pd.DataFrame(data)

    # Parse actions field to extract link_click
    def extract_action(action_list, action_type):
        for action in action_list:
            if action['action_type'] == action_type:
                return action['value']
        return 0

    fb_ads_data['link_click'] = fb_ads_data['actions'].apply(lambda x: extract_action(x, 'link_click') if isinstance(x, list) else 0)
    fb_ads_data['comments'] = fb_ads_data['actions'].apply(lambda x: extract_action(x, 'comment') if isinstance(x, list) else 0)
    fb_ads_data['leads'] = fb_ads_data['actions'].apply(lambda x: extract_action(x, 'lead') if isinstance(x, list) else 0)
    fb_ads_data['outbound_clicks'] = fb_ads_data['outbound_clicks'].apply(lambda x: extract_action(x, 'outbound_click') if isinstance(x, list) else 0)
    
    fb_ads_data = fb_ads_data.rename(columns={'date_start': 'date'})
    fb_ads_data = fb_ads_data.sort_values(by='date', ascending=False)
    fb_ads_data = fb_ads_data[['date', 'account_currency', 'campaign_name', 'adset_name', 'ad_name','impressions', 'clicks', 'spend', 'reach', 'link_click', 'comments', 'leads', 'objective', 'outbound_clicks']]
    fb_ads_data.to_csv('test_Data.csv')
    return fb_ads_data

def main():
    access_token = os.getenv("fb_access_token")
    account_ids = [os.getenv('account_id_2'), os.getenv('account_id_4'), os.getenv('account_id_1')]
    for account_id in account_ids:
        data = fetch_meta_insight_data(access_token=access_token, account_id=account_id)
        print(data)
        sheet_name = f"meta_{account_id}"
        google_sheet_import(data, sheet_name)

if __name__ == '__main__':
    main()