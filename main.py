#! /home/devops/Projetos/FUB-analytics/venv/bin/python
"""
This script is part of an ETL project (extract, transform, load).
It's meant to be used to extract data from Meta Insights, transform the result
to a dataframe object and upload it to a Postgres database.

Python Client for Meta Graph API
https://developers.facebook.com/docs/graph-api/reference/insights
https://developers.facebook.com/docs/instagram-api/getting-started
https://developers.facebook.com/docs/instagram-api/reference/
https://developers.facebook.com/docs/instagram-api/guides/insights
https://developers.facebook.com/docs/instagram-api/reference/ig-user/insights
https://developers.facebook.com/tools/debug/accesstoken
"""
import os
import pandas as pd
import pendulum
import requests
import sys

from sqlalchemy import create_engine, types, Engine, MetaData, Table
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
    if len(sys.argv) != 3 and not any(os.environ.get(var) for var in ['VSCODE_PID', 'TERM_PROGRAM']):
        print("\nERROR - Missing parameter.",
              "INFO  - usage: './main.py start_date end_date'",
              "INFO  - dates must be passed on format 'YYYY-MM-DD'", "", sep="\n")
        sys.exit(1)

    try:
        if len(sys.argv) > 3:
            # Parse values from command line
            start_date = pendulum.parse(sys.argv[1]).start_of('day')
            end_date = pendulum.parse(sys.argv[2]).end_of('day')
        else:
            # Default values when running from an IDE
            start_date = pendulum.now().subtract(days=1).start_of('day')
            end_date = pendulum.now().subtract(days=1).end_of('day')

    except IndexError as err:
        print("ERROR -", err)

    # Query parameters
    accounts = ['FutureBrand São Paulo', 'Holos Media', 'Springpoint', 'Timelens']

    # API client setup
    secret_path = "facebook-business-api"  # HashiVault secret name
    _, secret = HashiVaultClient().get_secret(secret_path)
    client = MetaInsights(client_secret=secret)

    # FACEBOOK Insights
    fb_df = pd.DataFrame()
    for account in accounts:
        temp_df = client.get_fb_page_insights(account, since=start_date, until=end_date)
        fb_df = pd.concat([fb_df, temp_df], ignore_index=True)

    # FACEBOOK Posts
    fb_posts = pd.DataFrame()
    for account in accounts:
        temp_df = client.get_fb_post_info(account, limit=100)
        fb_posts = pd.concat([fb_posts, temp_df], ignore_index=True)

    # INSTAGRAM Daily Insights
    ig_base_df = pd.DataFrame()
    ig_detail_df = pd.DataFrame()
    for account in accounts:
        temp_base_df = client.get_ig_base_insights(account, since=start_date, until=end_date)
        ig_base_df = pd.concat([ig_base_df, temp_base_df], ignore_index=True)

        temp_detail_df = client.get_ig_detail_insights(account, since=start_date, until=end_date)
        ig_detail_df = pd.concat([ig_detail_df, temp_detail_df], ignore_index=True)

    # INSTAGRAM Lifetime Insights
    ig_lft_insights_df = pd.DataFrame()
    for account in accounts:
        temp_df = client.get_ig_lifetime_insights(account)
        ig_lft_insights_df = pd.concat([ig_lft_insights_df, temp_df], ignore_index=True)

    # SQLAlchemy engine setup and upsert to database
    secret_path = "engenharia@cluster-postgresql-0001-01"  # HashiVault secret name
    _, secret = HashiVaultClient().get_secret(secret_path)
    database = 'HC_FUTUREBRAND'
    uri = f'postgresql+psycopg2://{secret["user"]}:{secret["password"]}@{secret["host"]}:{secret["port"]}/{database}'
    engine = create_engine(uri)

    if fb_df.empty:
        print("INFO  - No Facebook Insights data to write to the database.")

    else:
        print("\n", fb_df)
        # fb_df.to_csv("./sample_fb_page_insights.csv")

        schema = 'public'
        table_name = 'facebook_insights'
        constraint_columns = ['end_time', 'page_id']

        client.upsert_df_into_postgres(engine, table_name, schema, fb_df, constraint_columns)

    if fb_posts.empty:
        print("INFO  - No Facebook Posts data to write to the database.")

    else:
        print(fb_posts)
        fb_posts.to_csv("./sample_fb_posts.csv")

        schema = 'public'
        table_name = 'facebook_posts'
        constraint_columns = ['id']

        client.upsert_df_into_postgres(engine, table_name, schema, fb_posts, constraint_columns)

    if ig_base_df.empty and ig_detail_df.empty:
        print("INFO  - No Instagram Daily Insights data to write to the database.")

    else:
        # Join the DataFrames
        ig_df = pd.merge(ig_base_df, ig_detail_df, on=['end_time', 'account_id'], how='inner').reset_index(drop=True)
        ig_df.rename(columns={'end_time': 'date', 'account_id': 'business_account_id',
                              'impressions_day': 'impressions', 'reach_day': 'reach'}, inplace=True)

        print("\n", ig_df)
        # ig_df.to_csv("./sample_instagram_insights.csv")

        schema = 'instagram'
        table_name = 'user_insights'
        constraint_columns = ['date', 'page_id']

        client.upsert_df_into_postgres(engine, table_name, schema, ig_df, constraint_columns)

    if ig_lft_insights_df.empty:
        print("INFO  - No Instagram Lifetime Insights data to write to the database.")

    else:
        print("\n", ig_lft_insights_df)
        # fb_df.to_csv("./sample_ig_lft_insights.csv")

        schema = 'instagram'
        table_name = 'user_lifetime_insights'
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
            'metric': ','.join(self.fb_page_params['metrics']),
            'period': 'day',
            'since': since_str,
            'until': until_str,
        }
        url = f'{base_url}{endpoint}'

        # print(f'Request endpoint: {url}')  # DEBUG
        # print(f'Request params: {params}')  # DEBUG

        # Make the HTTP request
        print(f"INFO  - Request for API endpoint '{endpoint}' since '{since_str}' until '{until_str}'")
        response = requests.get(url, params=params)
        response_json = response.json()

        if response_json.get('data') is None:
            print('INFO  - No records for selected date')
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

    def get_fb_post_info(self, account_name: str, limit: int = 100) -> pd.DataFrame:
        """
        Fetch Facebook posts basic info.

        Parameters:
        - object_id (str): Facebook account name.
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
            'period': 'lifetime',
            'limit': limit,
        }
        url = f'{base_url}{endpoint}'

        # print(f'Request endpoint: {url}')  # DEBUG
        # print(f'Request params: {params}')  # DEBUG

        # Make the HTTP request
        print(f"INFO  - Request for API endpoint '{endpoint}'")
        response = requests.get(url, params=params)
        response_json = response.json()

        if response_json.get('data') is None:
            print('INFO  - No records for selected date')
            return None

        # Create base dataframe
        df = pd.DataFrame(columns=[col for col in self.fb_post_params['fields']])

        # Convert the list into a DataFrame
        temp_df = pd.DataFrame(response_json['data'])
        df = pd.concat([df, temp_df], ignore_index=True)

        # Clean column text
        df['message'] = df['message'].apply(self.clean_text)
        df['story'] = df['story'].apply(self.clean_text)

        return df

    def get_ig_base_insights(self, account_name: str,
                             since: pendulum.datetime = None, until: pendulum.datetime = None) -> pd.DataFrame:
        """
        Fetch basic insights data ('impressions', 'reach') from Instagram business accounts.

        Parameters:
        - object_id (str): ID of the Facebook or Instagram page.
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

        print(f"INFO  - Fetching IG base insights for business_account linked to page '{account_name}'")

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

        if response_json.get('data') is None:
            print('INFO  - No records for selected date')
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
        df1 = pd.DataFrame(records)
        df1_pivoted = df1.pivot_table(index='end_time', aggfunc='first').reset_index()

        return df1_pivoted

    def get_ig_detail_insights(self, account_name: str,
                               since: pendulum.datetime = None, until: pendulum.datetime = None) -> pd.DataFrame:
        """
        Fetch detailed insights data from Instagram business accounts.

        Parameters:
        - object_id (str): Facebook account name that manage the desired Instagram business account.
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

        print(f"INFO  - Fetching IG detail insights for business_account linked to page '{account_name}'")

        # Construct the first API request URL
        base_url = 'https://graph.facebook.com/v19.0'
        endpoint = f'/{account_id}/insights'
        params = {
            'access_token': user_access_token,
            'metric': ','.join(self.ig_page_params['metrics']),
            'period': 'day',
            'since': since,
            'until': until,
        }
        url = f'{base_url}{endpoint}'

        # Make the HTTP request
        response = requests.get(url, params=params)
        response_json = response.json()

        if response_json.get('data') is None:
            print('INFO  - No records for selected date')
            return None

        # Flatten the data
        data = []
        for obj in response_json['data']:
            name = obj["name"],
            values = obj["values"],
            for entry in values[0]:
                end_time = entry["end_time"]
                value = entry["value"]
                data.append([name[0], end_time, value])

        # Pivot DataFrame
        df2 = pd.DataFrame(data, columns=['metric_name', 'end_time', 'value'])
        df2_pivoted = df2.pivot(index='end_time', columns='metric_name', values='value')

        # Reset the index to make end_time a column again
        df2_pivoted.reset_index(inplace=True)
        df2_pivoted['account_id'] = account_id
        df2_pivoted.columns.name = None

        return df2_pivoted

    def get_ig_lifetime_insights(self, account_name: str) -> pd.DataFrame:
        """
        Fetch lifetime insights data from Instagram business accounts.

        Parameters:
        - object_id (str): Facebook account name that manage the desired Instagram business account.

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

        print(f"INFO  - Fetching IG lifetime insights for business_account linked to page '{account_name}'")

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
        result = []

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

            result.append(temp_dict)

        df = pd.DataFrame(result)

        return df

    def clean_text(self, text: str) -> str:
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
            print(f"INFO - Sucess on insert operation with '{result.rowcount}' rows affected.")

        except Exception as e:
            session.rollback()
            print(f"ERROR - Error during upsert operation: {e}")
            raise

        finally:
            session.close()

    fb_page_params = {
        'metrics': [
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
        ],
        'dtype': {
            'end_time': types.TIMESTAMP(),
            'page_id': types.Integer(),

            'page_post_engagements': types.Integer(),
            'page_consumptions_unique': types.Integer(),
            'page_negative_feedback': types.Integer(),
            'page_fans_online_per_day': types.Integer(),

            'page_impressions': types.Integer(),
            'page_impressions_organic_v2': types.Integer(),
            'page_impressions_paid': types.Integer(),
            'page_impressions_unique': types.Integer(),

            'page_posts_impressions': types.Integer(),
            'page_posts_impressions_organic': types.Integer(),
            'page_posts_impressions_paid': types.Integer(),
            'page_posts_impressions_unique': types.Integer(),

            'page_actions_post_reactions_like_total': types.Integer(),
            'page_actions_post_reactions_love_total': types.Integer(),
            'page_actions_post_reactions_wow_total': types.Integer(),
            'page_actions_post_reactions_haha_total': types.Integer(),
            'page_actions_post_reactions_sorry_total': types.Integer(),
            'page_actions_post_reactions_anger_total': types.Integer(),
            'page_actions_post_reactions_total': types.JSON(),

            'page_fans': types.Integer(),
            'page_fan_adds': types.Integer(),
            'page_fan_removes': types.Integer(),

            'page_views_total': types.Integer(),
        }
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
        'metrics': [
            'post_impressions',
            'post_impressions_unique',
            'post_impressions_paid',
            'post_impressions_organic',
            'post_engaged_users',
            'post_clicks',
            'post_negative_feedback',
            'post_reactions_by_type_total',
            'post_activity_by_action_type',
        ]
    }

    ig_page_params = {
        'metrics': [
            'follower_count',

            'profile_views',
            'email_contacts',
            'get_directions_clicks',
            'phone_call_clicks',
            'text_message_clicks',
            'website_clicks',
        ],
        'dtype': {
            'impressions': types.Integer(),
            'impressions_week': types.Integer(),
            'impressions_days_28': types.Integer(),
            'reach': types.Integer(),
            'reach_week': types.Integer(),
            'reach_days_28': types.Integer(),
            'follower_count': types.Integer(),
            'profile_views': types.Integer(),
            'email_contacts': types.Integer(),
            'get_directions_clicks': types.Integer(),
            'phone_call_clicks': types.Integer(),
            'text_message_clicks': types.Integer(),
            'website_clicks': types.Integer(),
        }
    }


if __name__ == "__main__":
    main()
