from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
from datetime import datetime, timedelta
import json
import pandas as pd
import os


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


# Function to publish the transformed data to Kafka
def publish_flight_data_event_message(producer, df):
    # For each rows in DF, produce PAX messages to Kafka topic
    for _, row in df.iterrows():
        event_message = {
            'flight_date': row['Date'].strftime('%Y-%m-%d'),
            'airline': row['Airline'],
            'flight_number': int(row['Flight Number']),
            'boarded_y': int(row['Boarded Y']),
            'capacity_physical_y': int(row['Capacity Physical Y']),
            'capacity_saleable_y': int(row['Capacity Saleable Y'])
        }
        print(f"Publishing message: {event_message}")

        # Send the message to Kafka
        producer.produce(
            topic=os.getenv('KAFKA_TOPIC_PAX'),
            value=json.dumps(event_message),
            callback=delivery_report
        )

        # Wait for any outstanding messages to be delivered
        producer.flush()


# Process each files in import data folders
def process_ingest_data():
    # Get Kafka producer
    producer = kafka_producer()

    # Directory path
    folder_path = '/opt/airflow/data/ingest/pax'

    # List all CSV files in the directory
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

    # Loop through each file and process
    for csv_file in csv_files:
        file_path = os.path.join(folder_path, csv_file)

        # Considering input data is in utf-16le encoding ONLY ALL THE TIME,
        # Similar to the data set provided in sample
        df = pd.read_csv(file_path, encoding='utf-16le')

        # Populate airline if value doesnt exist
        if 'Airline' not in df.columns:
            df['Airline'] = 'AB'
        else:
            df['Airline'] = df['Airline'].fillna('AirBharat')

        # Removing rows with the missing values
        df.dropna(inplace=True)

        df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)

        # Convert Flight Number to int
        df['Flight Number'] = df['Flight Number'].astype(int)

        print(f"Data from {csv_file}:")
        print(df)  # Print first few rows as an example
        print(df.describe())

        # Add logic to process or ingest the data as required
        publish_flight_data_event_message(producer, df)

        # Finally delete incoming file


# Default DAG Arguments
default_args = {
    'owner': 'airflow'
}

# Apache Airflow DAG
with DAG(
    'airline_data_processing_dag_pax',
    default_args=default_args,
    description='Apache Airflow DAG to process PAX Injest Data file',
    schedule_interval=timedelta(days=1), # Frequency can be adjusted according to need
    start_date=datetime(2024, 1, 1),
    tags=['airbharat', 'ingest-data-processing'],
    catchup=False,
) as dag:
    airline_data_processing_dag_pax_task = PythonOperator(
        task_id="airline_data_processing_dag_pax",
        python_callable=process_ingest_data
    )

# Executing Apache Airflow DAG
airline_data_processing_dag_pax_task
