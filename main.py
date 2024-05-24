#! /home/devops/Projetos/FUB-analytics/venv/bin/python
"""
This script is part of an ETL project (extract, transform, load).
It's meant to be used to extract data from Meta Insights, transform the result
to a dataframe object and upload it to a Postgres database.

Python Client for Meta Graph API
https://developers.facebook.com/docs/graph-api/reference/insights
https://developers.facebook.com/docs/instagram-api/reference/ig-media/
https://developers.facebook.com/docs/instagram-api/reference/ig-user/insights
https://developers.facebook.com/tools/debug/accesstoken
"""
import os
import pandas as pd
import pendulum
import requests
import sys

from sqlalchemy import create_engine, Engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from src._drv_hashicorp_vault import HashiVaultClient

SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))


def main():
    # ------------------------------------------------------------------------------------------
    # Main script
    # ------------------------------------------------------------------------------------------

    print(f"\nINFO  - BEGIN script at {pendulum.now().to_datetime_string()}")

    # Check if running from the command line and set parameters for start_date and end_date from CLI or from IDE
    if len(sys.argv) != 4 and not any(os.environ.get(var) for var in ['VSCODE_PID', 'TERM_PROGRAM']):
        print("\nERROR - Missing parameter.",
              "INFO  - usage: './main.py script_type start_date end_date'",
              "INFO  - dates must be passed on format 'YYYY-MM-DD'", "", sep="\n")
        sys.exit(1)

    try:
        if len(sys.argv) > 3:
            # Parse values from command line
            start_date = pendulum.parse(sys.argv[2]).start_of('day')
            end_date = pendulum.parse(sys.argv[3]).end_of('day')
            script_type = sys.argv[1]
        else:
            # Default values when running from an IDE
            start_date = pendulum.now().subtract(days=1).start_of('day')
            end_date = pendulum.now().subtract(days=1).end_of('day')
            script_type = 'instagram'

    except IndexError as err:
        print("ERROR -", err)

    # SQLAlchemy engine setup and upsert to database
    secret_path = "engenharia@cluster-postgresql-0001-01"  # HashiVault secret name
    _, secret = HashiVaultClient().get_secret(secret_path)
    database = 'HC_FUTUREBRAND'
    uri = f'postgresql+psycopg2://{secret["user"]}:{secret["password"]}@{secret["host"]}:{secret["port"]}/{database}'
    engine = create_engine(uri)

    # Query parameters
    accounts = ['FutureBrand São Paulo', 'Holos Media', 'Springpoint', 'Timelens']

    # API client setup
    secret_path = "facebook-business-api"  # HashiVault secret name
    _, secret = HashiVaultClient().get_secret(secret_path)
    client = MetaInsights(client_secret=secret)

    # Fetch FACEBOOK data
    if script_type == 'facebook':

        # FACEBOOK account insights
        fb_df = pd.DataFrame()
        for account in accounts:
            temp_df = client.get_fb_page_insights(account, since=start_date, until=end_date)
            fb_df = pd.concat([fb_df, temp_df], ignore_index=True)

        if fb_df.empty:
            print("INFO  - No Facebook Insights data to write to the database.")

        else:
            print("\n", fb_df)
            # fb_df.to_csv("./sample_fb_page_insights.csv")  # DEBUG

            schema = 'public'
            table_name = 'facebook_insights'
            constraint_columns = ['end_time', 'page_id']

            client.upsert_df_into_postgres(engine, table_name, schema, fb_df, constraint_columns)

        # FACEBOOK posts data
        fb_posts_df = pd.DataFrame()
        for account in accounts:
            temp_df = client.get_fb_post_data(account, limit=10)
            fb_posts_df = pd.concat([fb_posts_df, temp_df], ignore_index=True)

        if fb_posts_df.empty:
            print("INFO  - No Facebook Posts data to write to the database.")

        else:
            print(fb_posts_df)
            # fb_posts_df.to_csv("./sample_fb_posts_data.csv")  # DEBUG

            schema = 'public'
            table_name = 'facebook_posts'
            constraint_columns = ['id']

            client.upsert_df_into_postgres(engine, table_name, schema, fb_posts_df, constraint_columns)

        # FACEBOOK post insights
        fb_post_insights_df = pd.DataFrame()
        for account in accounts:
            temp_df = client.get_fb_post_insights(account, limit=10)
            fb_post_insights_df = pd.concat([fb_post_insights_df, temp_df], ignore_index=True)

        if fb_posts_df.empty:
            print("INFO  - No Facebook Posts Insights to write to the database.")

        else:
            print(fb_post_insights_df)
            # fb_posts_df.to_csv("./sample_fb_posts_insights.csv")  # DEBUG

            schema = 'public'
            table_name = 'facebook_posts_insights'
            constraint_columns = ['id']

            client.upsert_df_into_postgres(engine, table_name, schema, fb_post_insights_df, constraint_columns)

    # Fetch INSTAGRAM data
    if script_type == 'instagram':

        # # INSTAGRAM Posts data
        # ig_posts_df = pd.DataFrame()
        # for account in accounts:
        #     temp_base_df = client.get_ig_post_data(account, limit=10)
        #     ig_posts_df = pd.concat([ig_posts_df, temp_base_df], ignore_index=True)

        # if ig_posts_df.empty:
        #     print("INFO  - No Facebook Posts data to write to the database.")

        # else:
        #     print("\n", ig_posts_df)
        #     # ig_posts_df.to_csv("./sample_ig_posts_data.csv")  # DEBUG

        #     schema = 'public'
        #     table_name = 'ig_posts'
        #     constraint_columns = ['id']

        #     client.upsert_df_into_postgres(engine, table_name, schema, ig_posts_df, constraint_columns)

        # INSTAGRAM Posts Insights
        ig_posts_insights_df = pd.DataFrame()
        for account in accounts:
            temp_base_df = client.get_ig_post_insights(account, limit=100)
            ig_posts_insights_df = pd.concat([ig_posts_insights_df, temp_base_df], ignore_index=True)

        if ig_posts_insights_df.empty:
            print("INFO  - No Facebook Posts data to write to the database.")

        else:
            print("\n", ig_posts_insights_df)
            # ig_posts_df.to_csv("./sample_ig_posts_data.csv")  # DEBUG

            schema = 'public'
            table_name = 'ig_posts_insights'
            constraint_columns = ['id']

            client.upsert_df_into_postgres(engine, table_name, schema, ig_posts_insights_df, constraint_columns)

        # INSTAGRAM account daily insights
        ig_base_df = pd.DataFrame()
        ig_detail_df = pd.DataFrame()
        for account in accounts:
            temp_base_df = client.get_ig_base_insights(account, since=start_date, until=end_date)
            ig_base_df = pd.concat([ig_base_df, temp_base_df], ignore_index=True)

            temp_detail_df = client.get_ig_detail_insights(account, since=start_date, until=end_date)
            ig_detail_df = pd.concat([ig_detail_df, temp_detail_df], ignore_index=True)

        if ig_base_df.empty and ig_detail_df.empty:
            print("INFO  - No Instagram Daily Insights data to write to the database.")

        else:
            # Join the DataFrames
            ig_df = pd.merge(ig_base_df, ig_detail_df, on=['end_time',
                             'account_id'], how='inner').reset_index(drop=True)
            ig_df.rename(columns={'end_time': 'date', 'account_id': 'business_account_id',
                                  'impressions_day': 'impressions', 'reach_day': 'reach'}, inplace=True)

            print("\n", ig_df)
            # ig_df.to_csv("./sample_ig_insights.csv")  # DEBUG

            schema = 'public'
            table_name = 'ig_user_insights'
            constraint_columns = ['date', 'page_id']

            client.upsert_df_into_postgres(engine, table_name, schema, ig_df, constraint_columns)

        # INSTAGRAM demographic lifetime insights
        ig_lft_insights_df = pd.DataFrame()
        for account in accounts:
            temp_df = client.get_ig_lifetime_insights(account)
            ig_lft_insights_df = pd.concat([ig_lft_insights_df, temp_df], ignore_index=True)

        if ig_lft_insights_df.empty:
            print("INFO  - No Instagram Lifetime Insights data to write to the database.")

        else:
            print("\n", ig_lft_insights_df)
            # fb_df.to_csv("./sample_ig_lft_insights.csv")  # DEBUG

            schema = 'public'
            table_name = 'ig_user_lifetime_insights'
            constraint_columns = ['event_date', 'page_id', 'breakdown']

            client.upsert_df_into_postgres(engine, table_name, schema, ig_lft_insights_df, constraint_columns)


class MetaInsights():

    def __init__(self, client_secret: dict):
        """
        Initialize the Meta API Client.

        Parameters:
        - client_secret (dict): Meta API client secret.
        """
        self.app_id = client_secret["app_id"]
        self.app_secret = client_secret["app_secret"]
        # self.user_access_token = client_secret["access_token"]
        self.user_access_token = 'EAALY41zA9r0BO8nj2zdpprhytMoZAzR4wk2ZAqpm1ESG3RjziLarAMSxjLvUZBRMmVeEnZBZAy7kjGHaD5q19yTOZBjD39vMtuXXvlJx34olX2t3JedxA8EwYc5QY2O7gW7aL1S2TbbhSS6PkHCILJ0Q9Mw4MnEEhwOMZAA177Kg2oLXvZCyD5mOk1JN'  # noqa # User Token

    def get_account_id(self, account_name: str) -> dict:

        # Construct the API request URL
        base_url = 'https://graph.facebook.com/v19.0'
        endpoint = '/me/accounts'
        params = {
            'access_token': self.user_access_token,
            'fields': 'id,access_token,name,instagram_business_account',
        }
        url = f'{base_url}{endpoint}'

        # Make the HTTP request
        response = requests.get(url, params=params)

        data = response.json()
        result = {}

        for account in data['data']:
            if account['name'] == account_name:
                result = account
                break

        else:
            print(f"INFO  - Account name '{account_name}' not found under given access_token")

        # print(result)  # DEBUG
        return result

    def get_fb_page_insights(self, account_name: str,
                             since: pendulum.datetime = None, until: pendulum.datetime = None) -> pd.DataFrame:
        """
        Fetch insights data from Facebook and Instagram.

        Parameters:
        - object_id (str): Facebook account name.
        - since (str): Start date in YYYY-MM-DD format.
        - until (str): End date in YYYY-MM-DD format.

        Returns:
        - pd.DataFrame: DataFrame containing the retrieved insights data.
        """

        account = self.get_account_id(account_name)
        page_id = account.get("id")
        page_token = account.get("access_token")
        since_str = since.format('YYYY-MM-DD HH:mm:ss')
        until_str = until.format('YYYY-MM-DD HH:mm:ss')

        # Construct the API request URL
        base_url = 'https://graph.facebook.com/v19.0'
        endpoint = f'/{page_id}/insights'
        params = {
            'access_token': page_token,
            'metric': ','.join(self.fb_page_params['metric']),
            'period': 'day',
            'since': since_str,
            'until': until_str,
        }
        url = f'{base_url}{endpoint}'

        # Make the HTTP request
        print(f"INFO  - Request for '{account_name}' API endpoint '{endpoint}'")
        response = requests.get(url, params=params)
        response_json = response.json()

        if not isinstance(response_json, dict) or 'data' not in response_json:
            print('INFO  - No records for Facebook Page Insights selected parameters.')
            return None

        # Flatten the data
        records = []
        for obj in response_json['data']:
            name = obj["name"],
            values = obj["values"],
            for entry in values[0]:
                end_time = entry["end_time"]
                value = entry["value"]
                records.append([name[0], end_time, value])

        # Convert the list into a DataFrame
        df = pd.DataFrame(records, columns=['metric_name', 'end_time', 'value'])

        # Pivot the DataFrame to have multiple columns for each metric_name
        df_pivoted = df.pivot(index='end_time', columns='metric_name', values='value')

        # Reset the index to make end_time a column again
        df_pivoted.reset_index(inplace=True)
        df_pivoted['page_id'] = page_id
        df_pivoted['page_name'] = account_name
        df_pivoted.columns.name = None

        return df_pivoted

    def get_fb_post_data(self, account_name: str, limit: int = 10) -> pd.DataFrame:
        """
        Fetch Facebook posts basic info.

        Parameters:
        - account_name (str): Facebook account name.
        - limit (int): Number of last posts to be fetched.

        Returns:
        - pd.DataFrame: DataFrame containing the retrieved insights data.
        """

        account = self.get_account_id(account_name)
        page_id = account.get("id")
        page_token = account.get("access_token")

        # Construct the API request URL
        base_url = 'https://graph.facebook.com/v19.0'
        endpoint = f'/{page_id}/posts'
        params = {
            'access_token': page_token,
            'fields': ','.join(self.fb_post_params['fields']),
            'limit': limit,
        }
        url = f'{base_url}{endpoint}'

        # Make the HTTP request
        print(f"INFO  - Request for '{account_name}' API endpoint '{endpoint}'")
        response = requests.get(url, params=params)
        response_json = response.json()

        if not isinstance(response_json, dict) or 'data' not in response_json:
            print('INFO  - No records for Facebook posts selected parameters.')
            return None

        # Convert the list into a DataFrame
        df = pd.DataFrame(response_json['data'])
        df['page_id'] = page_id
        df['page_name'] = account_name

        # Clean column text if columns exist
        if 'message' in df.columns:
            df['message'] = df['message'].apply(self.clean_text)
        if 'story' in df.columns:
            df['story'] = df['story'].apply(self.clean_text)

        return df

    def get_fb_post_insights(self, account_name: str, limit: int = 10) -> pd.DataFrame:
        """
        Fetch Facebook posts insights info.

        Parameters:
        - account_name (str): Facebook account name.
        - limit (int): Number of last posts to be fetched.

        Returns:
        - pd.DataFrame: DataFrame containing the retrieved insights data.
        """

        account = self.get_account_id(account_name)
        page_id = account.get("id")
        page_token = account.get("access_token")

        # Construct the API request URL
        base_url = 'https://graph.facebook.com/v19.0'
        endpoint = f'/{page_id}/posts'
        params = {
            'access_token': page_token,
            'fields': 'id',
            'limit': limit,
        }
        url = f'{base_url}{endpoint}'

        # Make the HTTP request for the posts id list
        response = requests.get(url, params=params)
        response_json = response.json()

        post_id_list = [item["id"] for item in response_json['data']]

        # Make the HTTP request for the posts insights and concatenate results
        params.pop('fields', None)
        params.pop('limit', None)
        params['metric'] = ','.join(self.fb_post_params['metric']),
        records = []

        for index, post_id in enumerate(post_id_list, start=1):
            print(f"INFO  - {index}/{len(post_id_list)} fetching insights for '{account_name}' FB post '{post_id}'")

            endpoint = f'/{post_id}/insights'
            url = f'{base_url}{endpoint}'

            response = requests.get(url, params=params)
            post_insights = response.json()

            flattened_data = {
                'id': post_id,
                'period': 'lifetime',
            }

            for entry in post_insights['data']:
                # Flatten the data
                key: str = entry['name']
                value: int = entry['values'][0]['value']

                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        key = key.replace('_by_type_total', '').replace('_by_action_type', '')
                        flattened_data[f"{key}_{sub_key}"] = sub_value
                else:
                    flattened_data[key] = value

            records.append(flattened_data)

        # Convert the list into a DataFrame
        df = pd.DataFrame(records)
        df.rename(columns=lambda x: x.replace('post_', ''), inplace=True)

        return df

    def get_ig_base_insights(self, account_name: str,
                             since: pendulum.datetime = None, until: pendulum.datetime = None) -> pd.DataFrame:
        """
        Fetch basic insights data ('impressions', 'reach') from Instagram business accounts.

        Parameters:
        - account_name (str): Facebook account name that manage the desired Instagram business account.
        - since (str): Pendulum datetime obj for Start date.
        - until (str): Pendulum datetime obj for End date.

        Returns:
        - pd.DataFrame: DataFrame containing the retrieved insights data.
        """

        # print(f"INFO  - Fetching account data for {account_name}")
        account = self.get_account_id(account_name)
        page_id = account.get("id")
        account_id = account.get("instagram_business_account", {}).get("id")
        user_access_token = self.user_access_token

        if not account_id:
            print(f"INFO  - No instagram_business_account linked to page '{account_name}'")
            return None

        print(f"INFO  - Fetching IG base insights for '{account_name}'")

        # Construct the first API request URL
        base_url = 'https://graph.facebook.com/v19.0'
        endpoint = f'/{account_id}/insights'
        params = {
            'access_token': user_access_token,
            'metric': ','.join(['impressions', 'reach']),
            'period': ','.join(['day', 'week', 'days_28']),
            'since': since,
            'until': until,
        }
        url = f'{base_url}{endpoint}'

        # Make the HTTP request
        response = requests.get(url, params=params)
        response_json = response.json()

        if not isinstance(response_json, dict) or 'data' not in response_json:
            print('INFO  - No records for Instagram basic insights selected parameters.')
            return None

        # Flatten the data
        records = []
        for obj in response_json['data']:
            for value in obj['values']:
                records.append({
                    'end_time': value['end_time'],
                    f"{obj['name']}_{obj['period']}": value['value'],
                    'account_id': account_id,
                    'page_id': page_id
                })

        # Pivot DataFrame
        df = pd.DataFrame(records)
        df_pivoted = df.pivot_table(index='end_time', aggfunc='first').reset_index()

        return df_pivoted

    def get_ig_detail_insights(self, account_name: str,
                               since: pendulum.datetime = None, until: pendulum.datetime = None) -> pd.DataFrame:
        """
        Fetch detailed insights data from Instagram business accounts.

        Parameters:
        - account_name (str): Facebook account name that manage the desired Instagram business account.
        - since (str): Pendulum datetime obj for Start date.
        - until (str): Pendulum datetime obj for End date.

        Returns:
        - pd.DataFrame: DataFrame containing the retrieved insights data.
        """

        # print(f"INFO  - Fetching account data for {account_name}")
        account = self.get_account_id(account_name)
        account_id = account.get("instagram_business_account", {}).get("id")
        user_access_token = self.user_access_token

        if not account_id:
            print(f"INFO  - No instagram_business_account linked to page '{account_name}'")
            return None

        print(f"INFO  - Fetching IG detail insights for '{account_name}'")

        # Construct the first API request URL
        base_url = 'https://graph.facebook.com/v19.0'
        endpoint = f'/{account_id}/insights'
        params = {
            'access_token': user_access_token,
            'metric': ','.join(self.ig_page_params['metric']),
            'period': 'day',
            'since': since,
            'until': until,
        }
        url = f'{base_url}{endpoint}'

        # Make the HTTP request
        response = requests.get(url, params=params)
        response_json = response.json()

        if not isinstance(response_json, dict) or 'data' not in response_json:
            print('INFO  - No records for Instagram detail insights selected parameters.')
            return None

        # Flatten the data
        records = []
        for obj in response_json['data']:
            name = obj["name"],
            values = obj["values"],
            for entry in values[0]:
                end_time = entry["end_time"]
                value = entry["value"]
                records.append([name[0], end_time, value])

        # Pivot DataFrame
        df = pd.DataFrame(records, columns=['metric_name', 'end_time', 'value'])
        df_pivoted = df.pivot(index='end_time', columns='metric_name', values='value')

        # Reset the index to make end_time a column again
        df_pivoted.reset_index(inplace=True)
        df_pivoted['account_id'] = account_id
        df_pivoted.columns.name = None

        return df_pivoted

    def get_ig_lifetime_insights(self, account_name: str) -> pd.DataFrame:
        """
        Fetch lifetime insights data from Instagram business accounts.

        Parameters:
        - account_name (str): Facebook account name that manage the desired Instagram business account.

        Returns:
        - pd.DataFrame: DataFrame containing the retrieved insights data.
        """

        # print(f"INFO  - Fetching account data for {account_name}")
        account = self.get_account_id(account_name)
        page_id = account.get("id")
        account_id = account.get("instagram_business_account", {}).get("id")
        user_access_token = self.user_access_token

        if not account_id:
            print(f"INFO  - No instagram_business_account linked to page '{account_name}'")
            return None

        print(f"INFO  - Fetching IG lifetime insights for '{account_name}'")

        # Construct the first API request URL
        base_url = 'https://graph.facebook.com/v19.0'
        endpoint = f'/{account_id}/insights'
        params = {
            'access_token': user_access_token,
            'metric': 'follower_demographics',
            'period': 'lifetime',
            'breakdown': None,
            'metric_type': 'total_value',
        }
        url = f'{base_url}{endpoint}'

        breakdown_list = ['age', 'gender', 'city', 'country']
        records = []

        for item in breakdown_list:
            params['breakdown'] = item

            # Make the HTTP request
            response = requests.get(url, params=params)
            response_json = response.json()
            data = response_json["data"]

            # Flatten the data
            temp_dict = {
                'event_date': pendulum.today().format('YYYY-MM-DD'),
                'page_id': page_id,
                'business_account_id': account_id,
                'metric': data[0]['name'],
                'breakdown': data[0]['total_value']['breakdowns'][0]['dimension_keys'][0],
                'value': {item['dimension_values'][0]: item['value'] for item in data[0]['total_value']['breakdowns'][0]['results']}  # noqa
            }

            records.append(temp_dict)

        df = pd.DataFrame(records)

        return df

    def get_ig_post_data(self, account_name: str, limit: int = 10) -> pd.DataFrame:
        """
        Fetch Instagram posts basic info.

        Parameters:
        - account_name (str): Facebook account name that manage the desired Instagram business account.
        - limit (int): Number of last posts to be fetched.

        Returns:
        - pd.DataFrame: DataFrame containing the retrieved insights data.
        """

        account = self.get_account_id(account_name)
        account_id = account.get("instagram_business_account", {}).get("id")
        user_access_token = self.user_access_token

        # Construct the API request URL
        base_url = 'https://graph.facebook.com/v19.0'
        endpoint = f'/{account_id}/media'
        params = {
            'access_token': user_access_token,
            'fields': ','.join(self.ig_post_params['fields']),
            'limit': limit,
        }
        url = f'{base_url}{endpoint}'

        # Make the HTTP request
        print(f"INFO  - Request for '{account_name}' API endpoint '{endpoint}'")
        response = requests.get(url, params=params)
        response_json = response.json()

        if not isinstance(response_json, dict) or 'data' not in response_json:
            print('INFO  - No records for Instagram posts selected parameters.')
            return None

        # Convert the list into a DataFrame
        df = pd.DataFrame(response_json['data'])

        # Clean column text if columns exist
        if 'caption' in df.columns:
            df['caption'] = df['caption'].apply(self.clean_text)

        return df

    def get_ig_post_insights(self, account_name: str, limit: int = 10) -> pd.DataFrame:
        """
        Fetch Facebook posts insights info.

        Parameters:
        - account_name (str): Facebook account name.
        - limit (int): Number of last posts to be fetched.

        Returns:
        - pd.DataFrame: DataFrame containing the retrieved insights data.
        """

        account = self.get_account_id(account_name)
        account_id = account.get("instagram_business_account", {}).get("id")
        user_access_token = self.user_access_token

        # Construct the API request URL
        base_url = 'https://graph.facebook.com/v19.0'
        endpoint = f'/{account_id}/media'
        params = {
            'access_token': user_access_token,
            'fields': 'id,media_product_type,media_type',
            'limit': limit,
        }
        url = f'{base_url}{endpoint}'

        # Make the HTTP request for the posts id list
        response = requests.get(url, params=params)
        response_json = response.json()
        post_id_df = pd.DataFrame(response_json['data'])

        # Make the HTTP request for the posts insights and concatenate results
        params.pop('fields', None)
        params.pop('limit', None)
        records = []

        for index, row in post_id_df.iterrows():
            print(f"INFO  - {index+1}/{len(post_id_df)} fetching insights for '{account_name}' IG post '{row.id}'")

            endpoint = f'/{row.id}/insights'
            url = f'{base_url}{endpoint}'

            if row.media_product_type == 'REELS':
                params['metric'] = ','.join(self.ig_post_params['reels_metric'])
            elif row.media_product_type == 'FEED' and row.media_type == 'VIDEO':
                params['metric'] = ','.join(self.ig_post_params['feed_video_metric'])
            else:
                params['metric'] = ','.join(self.ig_post_params['feed_image_metric'])

            response = requests.get(url, params=params)
            post_insights = response.json()

            if not isinstance(post_insights, dict) or 'data' not in post_insights:
                continue

            flattened_data = {
                'id': row.id,
                'period': 'lifetime',
            }

            for entry in post_insights['data']:
                # Flatten the data
                key: str = entry['name']
                value: int = entry['values'][0]['value']
                flattened_data[key] = value

            records.append(flattened_data)

        # Convert the list into a DataFrame
        df = pd.DataFrame(records)

        return df

    def clean_text(self, text: str) -> str:
        '''
        Clean text removing line breaks, line feeds and encoding to UTF-8.
        Parameters:
        - text (str): Input text to be cleaned.

        Returns:
        - str: The cleaned result text.
        '''
        if text is None or pd.isna(text):
            return ''

        result = text.replace('\n', ' ').replace('\r', ' ')
        result = result.encode('utf-8', 'ignore').decode('utf-8')
        return result

    def upsert_df_into_postgres(self, sqlalchemy_engine: Engine, table_name: str, schema: str,
                                dataframe: pd.DataFrame, constraint_columns: list = None) -> str:
        '''
        Upserts a DataFrame into a PostgreSQL database table using a SQLAlchemy engine.

        Parameters:
        - sqlalchemy_engine (Engine): The SQLAlchemy engine connected to the PostgreSQL database.
        - table_name (str): The name of the table to upsert data into.
        - schema (str): The schema of the table.
        - dataframe (pd.DataFrame): The DataFrame containing the data to upsert.
        - constraint_columns (list): List of column names that form the unique constraint for conflict resolution.
                                    If None or empty, performs a simple insert.

        Returns:
        - str: The response message from the insert operation.
        '''

        Session = sessionmaker(bind=sqlalchemy_engine)
        session = Session()

        # Ensure correct data types for SQLAlchemy
        dataframe = dataframe.astype(object).where(pd.notna(dataframe), None)

        try:
            # Reflect the table structure from the database
            metadada = MetaData()
            table_obj = Table(table_name, metadada, schema=schema, autoload_with=sqlalchemy_engine)

            # Convert the DataFrame to a list of dictionaries
            data_to_upsert = dataframe.to_dict(orient='records')

            # Create an insert statement with ON CONFLICT clause
            insert_stmt = insert(table=table_obj).values(data_to_upsert)

            if constraint_columns:
                # Dynamically generate the set dictionary for the upsert
                update_columns = {col.name: getattr(insert_stmt.excluded, col.name)
                                  for col in table_obj.columns if col.name not in constraint_columns}

                # Define the update statement for the conflict
                update_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=constraint_columns,  # Unique constraint columns
                    set_=update_columns
                )

                # Execute the upsert
                result = session.execute(update_stmt)

            else:
                # Execute a simple insert without conflict resolution
                result = session.execute(insert_stmt)

            session.commit()
            print(f"INFO - Sucess on insert operation with '{result.rowcount}' rows affected.\n")

        except Exception as e:
            session.rollback()
            print(f"ERROR - Error during upsert operation: {e}")
            raise

        finally:
            session.close()

    fb_page_params = {
        'metric': [
            'page_total_actions',

            'page_post_engagements',
            'page_consumptions_unique',
            'page_negative_feedback',
            'page_fans_online_per_day',

            'page_impressions',
            'page_impressions_unique',
            'page_impressions_paid',
            'page_impressions_organic_v2',

            'page_posts_impressions',
            'page_posts_impressions_unique',
            'page_posts_impressions_paid',
            'page_posts_impressions_organic',

            'page_actions_post_reactions_like_total',
            'page_actions_post_reactions_love_total',
            'page_actions_post_reactions_wow_total',
            'page_actions_post_reactions_haha_total',
            'page_actions_post_reactions_sorry_total',
            'page_actions_post_reactions_anger_total',
            'page_actions_post_reactions_total',

            'page_fans',
            'page_fan_adds',
            'page_fan_removes',

            'page_views_total',
        ]
    }

    fb_post_params = {
        'fields': [
            'id',
            'story',
            'message',
            'full_picture',
            'permalink_url',
            'is_expired',
            'is_hidden',
            'is_published',
            'created_time',
            'updated_time',
        ],
        'metric': [
            'post_impressions',
            'post_impressions_unique',
            'post_impressions_paid',
            'post_impressions_organic',
            'post_engaged_users',
            'post_clicks',
            'post_negative_feedback',
            'post_reactions_by_type_total',
            'post_activity_by_action_type',
        ],
    }

    ig_page_params = {
        'metric': [
            'follower_count',
            'profile_views',
            'email_contacts',
            'get_directions_clicks',
            'phone_call_clicks',
            'text_message_clicks',
            'website_clicks',
        ]
    }

    ig_post_params = {
        'fields': [
            'id',
            'ig_id',
            'username',
            'caption',
            'media_product_type',
            'media_type',
            'media_url',
            'permalink',
            'timestamp',
        ],
        'feed_image_metric': [
            'impressions',
            'reach',
            'saved',
            'comments',
            'follows',
            'likes',
            'shares',
            'total_interactions',
        ],
        'feed_video_metric': [
            'impressions',
            'reach',
            'saved',
            'total_interactions',
        ],
        'reels_metric': [
            'comments',
            'likes',
            'plays',
            'reach',
            'saved',
            'shares',
            'total_interactions',
        ],
    }


if __name__ == "__main__":
    main()
