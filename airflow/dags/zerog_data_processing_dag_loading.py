from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
from datetime import datetime, timedelta
import gzip
import json
import os
import pandas as pd


# Kafka producer settings
KAFKA_CONFIG = {
    'bootstrap.servers': 'broker:29092',
    'debug': 'all'  # Enable debugging logs
}


# Kafka producer configuration
def kafka_producer():
    print("Intializing Kafka Producer")
    return Producer(KAFKA_CONFIG)


# Kafka producer delivery report function
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        # Turning off message delivery because of large number of messages received
        # print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
        None


# Data cleaning function
def clean_data(df):
    """Cleans the DataFrame and handles inconsistent data."""
    cleaned_data = []
    invalid_rows = []

    import re
    from datetime import datetime

    def clean_record(row):
        """Cleans individual record and handles inconsistent data."""
        try:
            # Required fields check
            required_fields = ["Flight Month/Year", "Airline", "Flightnumber", "Departure Unit", "Arrival Unit"]
            
            # Iterate through required fields and ensure they're present and not null
            for field in required_fields:
                if pd.isnull(row[field]) or row[field] == "":
                    raise ValueError(f"Missing or invalid required field: {field}")

            # Extract month and year from 'Flight Month/Year' field and populate 'flight_month' and 'flight_year'
            flight_month_year = str(row['Flight Month/Year'])
            if len(flight_month_year) == 6:
                # 6-digit case: MMYYYY
                flight_month = int(flight_month_year[:2])
                flight_year = int(flight_month_year[2:])
            elif len(flight_month_year) == 5:
                # 5-digit case: MYYYY
                flight_month = int(flight_month_year[0])
                flight_year = int(flight_month_year[1:])
            else:
                raise ValueError(f"Invalid Flight Month/Year format: {flight_month_year}")

            # Store the flight_month and flight_year as additional fields in the row
            row['flight_month'] = flight_month
            row['flight_year'] = flight_year

            # Clean Flight Date - convert to standard format if needed
            if not pd.isnull(row['Flight Date']) and row['Flight Date'] != "":
                try:
                    normalized_date = re.sub(r'[./]', '-', row['Flight Date'])
                    parsed_date = None
                    if re.match(r'\d{4}-\d{2}-\d{2}', normalized_date):  # YYYY-MM-DD
                        parsed_date = datetime.strptime(normalized_date, '%Y-%m-%d')
                    elif re.match(r'\d{2}-\d{2}-\d{4}', normalized_date):  # DD-MM-YYYY
                        parsed_date = datetime.strptime(normalized_date, '%d-%m-%Y')
                    elif re.match(r'\d{2}-\d{2}-\d{4}', normalized_date):  # MM-DD-YYYY
                        parsed_date = datetime.strptime(normalized_date, '%m-%d-%Y')

                    if parsed_date and parsed_date.month == flight_month and parsed_date.year == flight_year:
                        row['Flight Date'] = parsed_date.strftime('%Y-%m-%d')
                    else:
                        raise ValueError(f"Flight Date pattern does not match Month/Year: Normalized Date: {normalized_date}, M/Y: {flight_month}/{flight_year}")
                except ValueError as e:
                    print(f"Invalid Flight Date format: {row['Flight Date']}, M/Y: {flight_month}/{flight_year}, Error: {str(e)}")
                    # row['Flight Date'] = None
                    raise e

            # Extract price and currency from 'Sales and Services Invoice'
            if not pd.isnull(row['Sales and  Services Invoice']) and row['Sales and  Services Invoice'] != "":
                sales_services_split = row['Sales and  Services Invoice'].split(" ")
                row['sales_services_invoice'] = row['Sales and  Services Invoice']
                row['sales_services_invoice_value'] = float(sales_services_split[0].replace(",", "."))
                row['sales_services_invoice_currency'] = sales_services_split[1]
            else:
                row['sales_services_invoice'] = ""
                row['sales_services_invoice_value'] = float(0)
                row['sales_services_invoice_currency'] = ""

            # Extract price and currency from 'UAA Invoice'
            if not pd.isnull(row['UAA  Invoice']) and row['UAA  Invoice'] != "":
                uaa_invoice_split = row['UAA  Invoice'].split(" ")
                row['uaa_invoice'] = row['UAA  Invoice']
                row['uaa_invoice_value'] = float(uaa_invoice_split[0].replace(",", "."))
                row['uaa_invoice_currency'] = uaa_invoice_split[1]
            else:
                row['uaa_invoice'] = ""
                row['uaa_invoice_value'] = float(0)
                row['uaa_invoice_currency'] = ""

            return row  # Return the cleaned record

        except Exception as e:
            invalid_rows.append((row, str(e)))
            return None


    # Clean each record in the DataFrame by iterating through rows
    for idx, row in df.iterrows():
        cleaned_record = clean_record(row)
        if cleaned_record is not None:
            cleaned_data.append(cleaned_record)

    # Convert the cleaned data back into a DataFrame
    cleaned_df = pd.DataFrame(cleaned_data)

    if invalid_rows:
        print(f"Found {len(invalid_rows)} invalid rows. Skipping those.")
        for i, (record, error) in enumerate(invalid_rows[:5]):  # Only show first 5 invalid records for brevity
            print(f"Invalid Record {i+1}: {record}")
            print(f"Error: {error}\n")

    return cleaned_df


# Kafka producer function (unchanged from your original)
def publish_flight_data_event_message(producer, df):
    for _, row in df.iterrows():
        event_message = {
            'flight_month_year': row['Flight Month/Year'],
            'flight_month': row['flight_month'],
            'flight_year': row['flight_year'],
            'airline': row['Airline'],
            'flight_number': int(row['Flightnumber']),
            'departure_unit': row['Departure Unit'],
            'arrival_unit': row['Arrival Unit'],
            'flight_date': row['Flight Date'],
            'aircraft_type': int(row['Aircraft Type']),
            'leg': int(row['Leg']),
            'booking_class_cbase': row['Booking Class CBASE'],
            'bill_of_material': row['Bill of material'],
            'price': row['Price'],
            'invoice_quantity': row['Invoice Quantity'],
            'sales_services_invoice': row.get('sales_services_invoice', None),
            'sales_services_invoice_value': row.get('sales_services_invoice_value', None),
            'sales_services_invoice_currency': row.get('sales_services_invoice_currency', None),
            'uaa_invoice': row.get('uaa_invoice', None),
            'uaa_invoice_value': row.get('uaa_invoice_value', None),
            'uaa_invoice_currency': row.get('uaa_invoice_currency', None)
        }

        # print(f"Publishing message: {event_message}")
        producer.produce(
            topic=os.getenv('KAFKA_TOPIC_LOADING'),
            value=json.dumps(event_message),
            callback=delivery_report
        )

    # Wait for all messages to be delivered
    producer.flush()


# Function to read and clean the .json.gz file
def read_and_clean_gz_json(file_path):
    """Reads and cleans .json.gz file by replacing unwanted characters."""
    try:
        # Open the gzip file
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            # Load the raw file content as a string
            raw_content = f.read()

            # Replace unwanted delimiters like "\/" with "/"
            cleaned_content = raw_content.replace('\\/', '/')

            # Load the cleaned JSON data
            data = json.loads(cleaned_content)

        return data

    except Exception as e:
        print(f"Failed to read and clean file {file_path}: {str(e)}")
        return None

# Process and ingest data from .gz files
def process_ingest_data():
    # Directory path
    folder_path = '/opt/airflow/data/ingest/loading'

    # List all json.gz files in the directory
    json_files = [f for f in os.listdir(folder_path) if f.endswith('.gz')]

    # Initialize Kafka producer
    producer = kafka_producer()

    # Loop through each file and process
    for json_file in json_files:
        file_path = os.path.join(folder_path, json_file)

        print(f"Processing file: {file_path}")

        # Read and clean the data from the .gz JSON file
        data = read_and_clean_gz_json(file_path)

        if data is not None:
            # Convert to DataFrame for easier manipulation
            df = pd.DataFrame(data)

            # Clean and validate the DataFrame
            df_cleaned = clean_data(df)

            # Publish the cleaned data to Kafka
            publish_flight_data_event_message(producer, df_cleaned)
        else:
            print(f"Skipping file {json_file} due to errors.")


# Default DAG Arguments
default_args = {
    'owner': 'airflow'
}

# Apache Airflow DAG
with DAG(
    'airline_data_processing_dag_loading',
    default_args=default_args,
    description='Apache Airflow DAG to process Loading Injest Data file',
    schedule_interval=timedelta(days=1), # Frequency can be adjusted according to need
    start_date=datetime(2024, 1, 1),
    tags=['airbharat', 'ingest-data-processing'],
    catchup=False,
) as dag:
    airline_data_processing_dag_loading_task = PythonOperator(
        task_id="airline_data_processing_dag_loading",
        python_callable=process_ingest_data
    )

# Executing Apache Airflow DAG
airline_data_processing_dag_loading_task
