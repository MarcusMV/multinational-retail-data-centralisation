from dataclasses import dataclass
import pandas as pd
import sqlalchemy
import numpy as np
import math
import data_extraction
from database_utils import DatabaseConnector
from datetime import datetime, date, time


@dataclass
class DataCleaning:
    def clean_user_data(self, df):
        # Drop any records with missing/null values
        df = df.replace('NULL', np.nan)
        df = df.dropna(how='any')

        # Replace invalid characters
        pattern = r'[^0-9()+\-]'
        df['phone_number'] = df['phone_number'].str.replace(pattern, '-')

        # Remove if less than 7 digits (0-9)
        digit_counts = df['phone_number'].str.count('\d')
        df = df[digit_counts >= 7]

        # Length check country code and change dtype
        df['country_code'] = df['country_code'].map(lambda x: x[1:] if len(x) > 2 else x)
        df['country_code'].astype('category')

        # Convert date columns to datetime objects
        date_cols = ['date_of_birth', 'join_date']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        df = df.dropna(how='any')

        return df

    def clean_card_data(self, df):
        # Drop any records with missing/null values
        df = df.replace('NULL', np.nan)

        # Convert date column to date object
        df['date_payment_confirmed'] = pd.to_datetime(
            df['date_payment_confirmed'], errors='coerce').dt.date
        df = df.dropna()

        # Clean card_number of non-integer and check number length to card_provider
        df['card_number'] = df['card_number'].astype(str).str.replace('\D', '')
        # df['card_number'] = df['card_number'].astype('int64')

        # Check no length on card_provider col
        df['card_provider'] = df['card_provider'].astype('category')

        def clean_card_numbers(df):
            card_provider_lengths = {"VISA 19 digit": 19, "Diners Club / Carte Blanche": 14, "VISA 16 digit": 16,
                                     "JCB 16 digit": 16, "JCB 15 digit": 15, "Discover": 16, "VISA 13 digit": 13,
                                     "American Express": 15, "Mastercard": 16, "Maestro": 12}

            df['card_number'] = df.apply(lambda row: row['card_number'] if int(math.log10(
                int(row['card_number'])))+1 == card_provider_lengths[row['card_provider']] else None, axis=1)
            df = df.dropna(subset=['card_number'])

            return df

        df = clean_card_numbers(df)

        return df

    def clean_store_data(self, df):
        # Convert dtypes and clean strings
        df['opening_date'] = pd.to_datetime(
            df['opening_date'], errors='coerce').dt.date
        df['address'] = df['address'].replace(r'\n', ' ', regex=True)
        df['continent'] = df['continent'].replace(r'ee', '', regex=True)
        df['staff_numbers'] = pd.to_numeric(
            df['staff_numbers'], downcast='integer', errors='coerce')

        # Drop rows
        # df = df.replace('NULL', np.nan)
        # df = df.drop(['lat'], axis=1)
        df = df.dropna(subset=['staff_numbers'])

        # Convert to float
        df = df.astype({"longitude": "float", "latitude": "float"})

        return df

    def convert_product_weights(self, weight_str):
        # Remove non-numeric and non-decimal characters
        # weight_str = 

        # Get total if weight has 'x'
        if 'x' in weight_str:
            quantity, weight_per_item_str = weight_str.split('x')
            quantity = int(quantity)
            weight_per_item = float(''.join(c for c in weight_per_item_str if c.isdigit() or c == '.'))

            total_weight = quantity * weight_per_item
        else:
            total_weight = float(''.join(c for c in weight_str if c.isdigit() or c == '.'))

        if 'kg' in weight_str:
            return round(total_weight, 3)
        elif 'g' in weight_str:
            return round(total_weight / 1000, 3)
        elif 'ml' in weight_str:
            return round(total_weight / 1000, 3)
        else:
            return None

    def clean_products_data(self, df):
        df = df.copy()

        # Re-name and set index
        df = df.rename(columns={'Unnamed: 0': 'index'})
        df = df.set_index('index')

        # Convert date column to date object
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce').dt.date
        df = df.dropna()

        # Convert weights to float as kg
        df['weight'] = df['weight'].astype(str).apply(self.convert_product_weights)
        df = df.drop(df[df['weight'] > 100].index)
        df = df.dropna()

        # Length check and standardize other columns
        df = df.loc[~df['EAN'].astype(str).map(lambda x: len(x) != 13)]
        df = df.loc[~df['product_code'].astype(str).map(lambda x: len(x) != 11)]
        df['product_code'] = df['product_code'].str.upper()

        return df
    
    def clean_orders_data(self, df):
        df = df.copy()
        df = df.drop(['level_0', 'first_name', 'last_name', '1'], axis=1)

        # Standardize other columns
        df['product_code'] = df['product_code'].str.upper()
        df['store_code'] = df['store_code'].str.upper()
        df['product_quantity'] = df['product_quantity'].astype('int')
        df = df.dropna()

        return df
    
    def clean_dates_data(self, df):
        df = df.copy()

        # Convert to float, remove non-numeric
        df['day'] = pd.to_numeric(df['day'], errors='coerce')
        df['month'] = pd.to_numeric(df['month'], errors='coerce')
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['time_period'] = df['time_period'].astype('category')
        df = df.dropna()

        # Convert to int
        df[['day', 'month', 'year']] = df[['day', 'month', 'year']].astype('int64')

        # Validate day, month, year values
        df = df.loc[df['day'].map(lambda x: x >= 1 and x <= 31)]
        df = df.loc[df['month'].map(lambda x: x >= 1 and x <= 12)]
        df = df.loc[df['year'].map(lambda x: x >= 1992 and x <= 2023)]

        # Combine columns for complete datetime
        df['date'] = pd.to_datetime(df[['year', 'month', 'day']].astype(str).agg('-'.join, axis=1) + ' ' + df['timestamp'])

        return df


cleaner = DataCleaning()
connector = DatabaseConnector()

# Clean and upload users data
# users = data_extraction.legacy_users
# cleaned_users = cleaner.clean_user_data(users)
# connector.upload_to_db(cleaned_users, 'dim_users')

# Clean and upload card_details
# card_details = data_extraction.card_details
# cleaned_card_details = cleaner.clean_card_data(card_details)
# connector.upload_to_db(cleaned_card_details, 'dim_card_details')
# print(cleaned_card_details)

# Clean and upload stores_data
# stores_data = data_extraction.stores
# cleaned_stores_data = cleaner.clean_store_data(stores_data)
# connector.upload_to_db(cleaned_stores_data, 'dim_store_details')
# print(cleaned_stores_data.to_string())

# Clean and upload products data
# products = data_extraction.products
# cleaned_products_data = cleaner.clean_products_data(products)
# connector.upload_to_db(cleaned_products_data, 'dim_products')

# Clean and upload orders data
orders = data_extraction.orders
cleaned_orders_data = cleaner.clean_orders_data(orders)
connector.upload_to_db(cleaned_orders_data, 'orders_table_web')
# print(cleaned_orders_data.to_string())

# Clean and upload the date events data
# dates = data_extraction.dates
# cleaned_dates_data = cleaner.clean_dates_data(dates)
# connector.upload_to_db(cleaned_dates_data, 'dim_date_times')
# print(cleaned_dates_data.dtypes)