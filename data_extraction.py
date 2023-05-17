from dataclasses import dataclass
from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import boto3


@dataclass
class DataExtractor:
    def read_rds_table(self, connector, table):
        creds = connector.read_db_creds('./db_creds.yaml')
        engine = connector.init_db_engine(creds)

        table = pd.read_sql_table(table, engine)
        return table

    def retrieve_pdf_data(self, pdf_path):
        dfs = tabula.read_pdf(pdf_path, pages='all')

        # Concatenate dfs list to signle df
        df = pd.concat(dfs, ignore_index=True)
        return df

    def list_number_of_stores(self, endpoint, header):
        response = requests.get(endpoint, headers=header)
        if response.status_code == 200:
            return response.json()['number_stores']
        else:
            return None

    def retrieve_stores_data(self, endpoint_base, header):
        no_of_stores = self.list_number_of_stores(
            'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers)
        stores = []

        for i in range(1, no_of_stores + 1):
            endpoint = endpoint_base + str(i)
            response = requests.get(endpoint, headers=header)
            data = response.json()

            if isinstance(data, dict):
                stores.append(data)

        stores_df = pd.DataFrame(stores)
        # stores_df = stores_df.set_index('index')
        return stores_df

    def extract_from_s3(self, s3_address):
        s3 = boto3.client('s3')

        # Get bucket and file name from s3 address
        s3_address = s3_address.replace('s3://', '')
        bucket_name = s3_address.split('/')[0]
        file_name = s3_address.split('/')[1]

        # Download file from bucket
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        df = pd.read_csv(response['Body'])

        return df

    def extract_from_endpoint(self, endpoint, header):
        response = requests.get(endpoint, headers=header)
        if response.status_code == 200:
            data = response.json()
            return pd.DataFrame(data)
        else:
            return None


connector = DatabaseConnector()
extractor = DataExtractor()

# legacy_users = extractor.read_rds_table(connector, 'legacy_users')
# card_details = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
# print(card_details.to_string())

headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
# no_of_stores = extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers)
# print(no_of_stores)

stores = extractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/', headers)
products = extractor.extract_from_s3('s3://data-handling-public/products.csv')
orders = extractor.read_rds_table(connector, 'orders_table')
dates = extractor.extract_from_endpoint('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json', headers)
# print(dates)

# print(orders.to_string())