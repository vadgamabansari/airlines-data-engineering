from cassandra import WriteTimeout, WriteFailure, OperationTimedOut
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import SimpleStatement
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer
import json
import os

# Load environment variables from .env file
load_dotenv()

# Connect to Cassandra
cassandra_host = os.getenv('CASSANDRA_HOST', 'localhost')
cassandra_keyspace = os.getenv('CASSANDRA_KEYSPACE', 'your_keyspace')
cassandra_table = os.getenv('CASSANDRA_TABLE_PAX', 'pax')
kafka_topic_name = os.getenv('KAFKA_TOPIC_PAX')

cluster = Cluster([cassandra_host])
session = cluster.connect(cassandra_keyspace)

# Define the CQL query for inserting or updating data into Cassandra
insert_query = SimpleStatement(f"""
    INSERT INTO {cassandra_table} 
    (flight_date, airline, flight_number, boarded_y, capacity_physical_y, capacity_saleable_y)
    VALUES (%s, %s, %s, %s, %s, %s)
""")

# Define the Kafka consumer
consumer = KafkaConsumer(
    kafka_topic_name,  # Kafka topic to consume from
    bootstrap_servers=[os.getenv('KAFKA_BROKER', 'localhost:9092')],  # Kafka broker address from env
    auto_offset_reset='earliest',  # Start from the earliest message if no offset is committed
    enable_auto_commit=True,  # Automatically commit offsets after consuming
    group_id=f'airline_{cassandra_table}_consumer_group',  # Consumer group ID for offset tracking
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserializer to convert byte messages to JSON
)

# Function to parse and validate message fields
def parse_message(message):
    try:
        # Convert flight_date from string to datetime.date
        flight_date_str = message.get('flight_date')
        flight_date = datetime.strptime(flight_date_str, '%Y-%m-%d').date()  # Convert string to date        
        airline = message.get('airline')
        flight_number = int(message.get('flight_number'))
        boarded_y = int(message.get('boarded_y'))
        capacity_physical_y = int(message.get('capacity_physical_y'))
        capacity_saleable_y = int(message.get('capacity_saleable_y'))
                
        return (
            flight_date, airline, flight_number, boarded_y, capacity_physical_y, capacity_saleable_y
        )
    except Exception as e:
        # Log error and skip the record if there's an issue
        print(f"Error parsing message: {e}, Message: {message}")
        return None
    

# Generate Unique Key
def generate_unique_key(message):
    return f"{message.get('flight_date')}-{message.get('airline')}-{message.get('flight_number')}"


# Return processed message data info
def get_processed_message_data_info(message):
    return f"{message.get('airline')} {message.get('flight_number')} / {message.get('flight_date')}"


# Consume and print messages from Kafka
print(f"Listening for messages on {kafka_topic_name}...")

messages_received = 0
messages_added_to_db = 0

print(f"Records received vs processed: ({messages_received} / {messages_added_to_db})")

try:
    for message in consumer:
        event_message = message.value  # Access the message content (in JSON format)
        
        # Parse and validate message fields
        parsed_message = parse_message(event_message)
        
        if parsed_message:
            messages_received += 1
            try:
                # Insert the message into Cassandra
                session.execute(insert_query, parsed_message)
                messages_added_to_db += 1
                print(f"({messages_received} / {messages_added_to_db}) - " + 
                    f"{cassandra_table} Record inserted/updated into Cassandra for: {get_processed_message_data_info(event_message)}"
                    f" - Unique Key: ({generate_unique_key(event_message)})")
            except (WriteTimeout, WriteFailure) as e:
                # Handle specific Cassandra write errors
                print(f"Write operation failed: {e}")            
            except NoHostAvailable as e:
                # Handle when no Cassandra host is available
                print(f"No host available: {e}")
            except OperationTimedOut as e:
                # Handle operation timeout errors
                print(f"Operation timed out: {e}")
            except Exception as e:
                # Handle any other exceptions
                print(f"Query execution failed: {e}")
        else:
            print("Skipping invalid message.")
finally:
    # Close the Cassandra connection when done
    print(f"Shutting down consumer registered for {kafka_topic_name}.")
    cluster.shutdown()
