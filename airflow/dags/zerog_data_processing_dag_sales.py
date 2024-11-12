from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
from datetime import datetime, timedelta
import json
import pandas as pd
import os
import re
from decimal import Decimal


# Kafka producer settings
KAFKA_CONFIG = {
    'bootstrap.servers': 'broker:29092',
    'debug': 'all'  # Enable debugging logs
}


# Kafka producer configuration
def kafka_producer():
    print("Intializing Kafka Producer")
    return Producer(KAFKA_CONFIG)


# Kafka topic producer delivery report function
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def remove_special_characters(text):
    if isinstance(text, str):
        # Use regex to remove non-ASCII characters
        return re.sub(r'[^\x00-\x7F]+', '', text)
    return text


# Function to publish the transformed data to Kafka
def publish_flight_data_event_message(df):
    # Get Kafka producer
    producer = kafka_producer()

    for _, row in df.iterrows():
        event_message = {
            'rank': int(row['Rank']),
            'transaction_id': row['TransactionID'],
            'origin': row['Origin'],
            'destination': row['Destination'],
            'flight': row['Flight'],
            'flight_date': row['FlightDate'],
            'short_flight_date': row['ShortFlightDate'],
            'flight_date_time': row['FlightDateTime'],
            'transaction_date': row['TransactionDate'],
            'short_transaction_date': row['ShortTransactionDate'],
            'transaction_date_time': row['TransactionDateTime'],
            'aircraft_registration': row['AircraftRegistration'],
            'aircraft_type': row['AircraftType'],
            'crew_member': row['CrewMember'],
            'product_type': row['ProductType'],
            'product_code': row['ProductCode'],
            'quantity': int(row['Quantity']),
            'cost_price': int(row['CostPrice']),
            'net_sales': int(row['NetSales']),
            'vat': int(row['VAT']),
            'gross_sales': int(row['GrossSales']),
            'total_sales': Decimal(row['TotalSales']),
            'cash': row['Cash'],
            'card': row['Card'],
            'voucher': row['Voucher'],
            'payment_details': row['PaymentDetails'],
            'reason': row['Reason'],
            'voided': row['Voided'],
            'device_code': int(row['DeviceCode']),
            'payment_status': row['PaymentStatus'],
            'base_currency_price': Decimal(row['BaseCurrencyPrice']),
            'sale_item_id': int(row['SaleItemID']),
            'sale_item_status': int(row['SaleItemStatus']),
            'vat_rate_value': int(row['VatRateValue']),
            'crew_base_code': row['CrewBaseCode'],
            'refund_reason': row['RefundReason'],
            'payment_id': int(row['PaymentId']),
            'price_type': row['PriceType'],
            'seat_number': row['SeatNumber'],
            'passenger_count': int(row['PassengerCount']),
            'schedule_actual_departure_time': row['ScheduleActualDepartureTime'],
            'schedule_actual_arrival_time': row['ScheduleActualArrivalTime'],
            'sale_type_name': row['SaleTypeName'],
            'price_override_reason': row['PriceOverrideReason'],
            'discount_type': row['DiscountType'],
            'haul_type': row['HaulType'],
            'sale_upload_date': row['SaleUploadDate'],
            'sale_upload_time': row['SaleUploadTime']
        }

        print(f"Publishing message: {event_message}")

        # Send the message to Kafka
        producer.produce(
            topic=os.getenv('KAFKA_TOPIC_sales'),
            value=json.dumps(event_message),
            callback=delivery_report
        )

        # Wait for any outstanding messages to be delivered
        producer.flush()


# Process each files in import data folders
def process_ingest_data():
    # Directory path
    folder_path = '/opt/airflow/data/ingest/sales'

    # List all xlsx files in the directory
    xlsx_files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]

    # Loop through each file and process
    for xlsx_file in xlsx_files:

        file_path = os.path.join(folder_path, xlsx_file)

        df = pd.read_excel(file_path)

        if 'FlightDate' in df.columns:
            df['FlightDate'] = pd.to_datetime(df['FlightDate'], format='%m/%d/%Y %I:%M:%S %p')

        if 'ShortFlightDate' in df.columns:
            df['ShortFlightDate'] = pd.to_datetime(df['ShortFlightDate'], format='%d/%m/%Y').dt.date

        if 'FlightDateTime' in df.columns:
            df['FlightDateTime'] = pd.to_datetime(df['FlightDateTime'], format='%H:%M').dt.time

        if 'TransactionDate' in df.columns:
            df['TransactionDate'] = pd.to_datetime(df['TransactionDate'], utc=True, format='ISO8601')

        if 'ShortTransactionDate' in df.columns:
            df['ShortTransactionDate'] = pd.to_datetime(df['ShortTransactionDate'], format='%d/%m/%Y').dt.date

        if 'TransactionDateTime' in df.columns:
            df['TransactionDateTime'] = pd.to_datetime(df['TransactionDateTime'], format='%H:%M').dt.time

        if 'TotalSales' in df.columns:
            df['TotalSales'] = df['TotalSales'].round(2)

        if 'BaseCurrencyPrice' in df.columns:
            df['BaseCurrencyPrice'] = df['BaseCurrencyPrice'].round(2)

        if 'PaymentId' in df.columns:
            df['PaymentId'] = df['PaymentId'].astype('Int64')

        if 'ScheduleActualDepartureTime' in df.columns:
            df['ScheduleActualDepartureTime'] = pd.to_datetime(df['ScheduleActualDepartureTime'], format='%H:%M').dt.time

        if 'ScheduleActualArrivalTime' in df.columns:
            df['ScheduleActualArrivalTime'] = pd.to_datetime(df['ScheduleActualArrivalTime'], format='%H:%M').dt.time

        if 'SaleUploadDate' in df.columns:
            df['SaleUploadDate'] = pd.to_datetime(df['SaleUploadDate'], format='%d/%m/%Y').dt.date

        if 'SaleUploadTime' in df.columns:
            df['SaleUploadTime'] = pd.to_datetime(df['SaleUploadTime'], format='%H:%M').dt.time

        # Remove column 'Unnamed: 55'
        if 'Unnamed: 55' in df.columns:
            df.drop('Unnamed: 55', axis=1, inplace=True)

        # Replace missing/null values in the 'Destination' column with 'Unknown'
        if 'Destination' in df.columns:
            df['Destination'] = df['Destination'].fillna('Unknown')

        # Replace missing/null values in the 'Reason' column with 'Unknown'
        if 'Reason' in df.columns:
            df['Reason'] = df['Reason'].fillna('Unknown')

        # Replace missing/null values in the 'Paymentstatus' column with 'Unknown'
        if 'PaymentStatus' in df.columns:
            df['PaymentStatus'] = df['PaymentStatus'].fillna('Unknown')

        # Replace missing/null values in the 'RefundReason' column with 'Unknown'
        if 'RefundReason' in df.columns:
            df['RefundReason'] = df['RefundReason'].apply(remove_special_characters)
            df['RefundReason'] = df['RefundReason'].fillna('Unknown')

        # Replace NaN values with a default value (e.g., 0.0) for float64 type column
        if 'PaymentId' in df.columns:
            df['PaymentId'] = df['PaymentId'].fillna(0.0)

        # Replace missing values with None
        if 'ScheduleActualDepartureTime' in df.columns:
            df['ScheduleActualDepartureTime'] = df['ScheduleActualDepartureTime'].where(pd.notnull(df['ScheduleActualDepartureTime']), None)
            df['ScheduleActualDepartureTime'] = df['ScheduleActualDepartureTime'].apply(lambda x: None if pd.isna(x) else x)

        # Replace missing values with None
        if 'ScheduleActualArrivalTime' in df.columns:
            df['ScheduleActualArrivalTime'] = df['ScheduleActualArrivalTime'].where(pd.notnull(df['ScheduleActualArrivalTime']), None)
            df['ScheduleActualArrivalTime'] = df['ScheduleActualArrivalTime'].apply(lambda x: None if pd.isna(x) else x)

        # Convert the 'PriceOverrideReason', 'DiscountType', 'HaulType' column from float64 to string and replace missing values with 'Unknown'
        if 'PriceOverrideReason' in df.columns:
            df['PriceOverrideReason'] = df['PriceOverrideReason'].fillna('Unknown').astype(str)

        if 'DiscountType' in df.columns:
            df['DiscountType'] = df['DiscountType'].fillna('Unknown').astype(str)

        if 'HaulType' in df.columns:
            df['HaulType'] = df['HaulType'].fillna('Unknown').astype(str)

        # Add logic to process or ingest the data as required
        publish_flight_data_event_message(df)


# Default DAG Arguments
default_args = {
    'owner': 'airflow'
}

# Apache Airflow DAG
with DAG(
    'airline_data_processing_dag_sales',
    default_args=default_args,
    description='Apache Airflow DAG to process Sales Injest Data file',
    schedule_interval=timedelta(days=1), # Frequency can be adjusted according to need
    start_date=datetime(2024, 1, 1),
    tags=['airbharat', 'ingest-data-processing'],
    catchup=False,
) as dag:
    airline_data_processing_dag_sales_task = PythonOperator(
        task_id="airline_data_processing_dag_sales",
        python_callable=process_ingest_data
    )

# Executing Apache Airflow DAG
airline_data_processing_dag_sales_task
