from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from dotenv import load_dotenv
import json
import os

# Load environment variables from .env file
load_dotenv()

# Connect to Cassandra
cassandra_host = os.getenv('CASSANDRA_HOST', 'localhost')
cassandra_keyspace = os.getenv('CASSANDRA_KEYSPACE', 'your_keyspace')
cassandra_table = os.getenv('CASSANDRA_TABLE_SALES', 'sales')

cluster = Cluster([cassandra_host])
session = cluster.connect(cassandra_keyspace)

# Define the CQL query for inserting or updating data into Cassandra
insert_query = SimpleStatement(f"""
    INSERT INTO {cassandra_table} 
    (rank, transaction_id, origin, destination, flight, flight_date, short_flight_date, flight_date_time, transaction_date, short_transaction_date, transaction_date_time, aircraft_registration, aircraft_type, crew_member, product_type, product_code, quantity, cost_price, net_sales, vat, gross_sales, total_sales, cash, card, voucher, payment_details, reason, voided, device_code, payment_status, base_currency_price, sale_item_id, sale_item_status, vat_rate_value, crew_base_code, refund_reason, payment_id, price_type, seat_number, passenger_count, schedule_actual_departure_time, schedule_actual_arrival_time, sale_type_name, price_override_reason, discount_type, haul_type, sale_upload_date, sale_upload_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, vat, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

# Define the Kafka consumer
consumer = KafkaConsumer(
    os.getenv('KAFKA_TOPIC_SALES'),  # Kafka topic to consume from
    bootstrap_servers=[os.getenv('KAFKA_BROKER', 'localhost:9092')],  # Kafka broker address from env
    auto_offset_reset='earliest',  # Start from the earliest message if no offset is committed
    enable_auto_commit=True,  # Automatically commit offsets after consuming
    group_id='airline_sales_consumer_group',  # Consumer group ID for offset tracking
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserializer to convert byte messages to JSON
)

# Consume and print messages from Kafka
print(f"Listening for messages on {os.getenv('KAFKA_TOPIC_SALES')}...")

for message in consumer:
    event_message = message.value  # Access the message content (in JSON format)
    
    # Extract fields from the message
    rank = event_message['rank'], 
    transaction_id = event_message['transaction_id'], 
    origin = event_message['origin'], 
    destination = event_message['destination'], 
    flight = event_message['flight'], 
    flight_date = event_message['flight_date'], 
    short_flight_date = event_message['short_flight_date'], 
    flight_date_time = event_message['flight_date_time'], 
    transaction_date = event_message['transaction_date'], 
    short_transaction_date = event_message['short_transaction_date'], 
    transaction_date_time = event_message['transaction_date_time'], 
    aircraft_registration = event_message['aircraft_registration'], 
    aircraft_type = event_message['aircraft_type'], 
    crew_member = event_message['crew_member'], 
    product_type = event_message['product_type'], 
    product_code = event_message['product_code'], 
    quantity = event_message['quantity'], 
    cost_price = event_message['cost_price'], 
    net_sales = event_message['net_sales'], 
    vat = event_message['vat'], 
    gross_sales = event_message['gross_sales'], 
    total_sales = event_message['total_sales'], 
    cash = event_message['cash'], 
    card = event_message['card'], 
    voucher = event_message['voucher'], 
    payment_details = event_message['payment_details'], 
    reason = event_message['reason'], 
    voided = event_message['voided'], 
    device_code = event_message['device_code'], 
    payment_status = event_message['payment_status'], 
    base_currency_price = event_message['base_currency_price'], 
    sale_item_id = event_message['sale_item_id'], 
    sale_item_status = event_message['sale_item_status'], 
    vat_rate_value = event_message['vat_rate_value'], 
    crew_base_code = event_message['crew_base_code'], 
    refund_reason = event_message['refund_reason'], 
    payment_id = event_message['payment_id'], 
    price_type = event_message['price_type'], 
    seat_number = event_message['seat_number'], 
    passenger_count = event_message['passenger_count'], 
    schedule_actual_departure_time = event_message['schedule_actual_departure_time'], 
    schedule_actual_arrival_time = event_message['schedule_actual_arrival_time'], 
    sale_type_name = event_message['sale_type_name'], 
    price_override_reason = event_message['price_override_reason'], 
    discount_type = event_message['discount_type'], 
    haul_type = event_message['haul_type'], 
    sale_upload_date = event_message['sale_upload_date'], 
    sale_upload_time = event_message['sale_upload_time']
    
    # Print the message for debugging purposes
    print(f"Received message: {event_message}")
    
    # Insert or update the message in Cassandra
    session.execute(insert_query, (rank, transaction_id, origin, destination, flight, flight_date, short_flight_date, flight_date_time, transaction_date, short_transaction_date, transaction_date_time, aircraft_registration, aircraft_type, crew_member, product_type, product_code, quantity, cost_price, net_sales, vat, gross_sales, total_sales, cash, card, voucher, payment_details, reason, voided, device_code, payment_status, base_currency_price, sale_item_id, sale_item_status, vat_rate_value, crew_base_code, refund_reason, payment_id, price_type, seat_number, passenger_count, schedule_actual_departure_time, schedule_actual_arrival_time, sale_type_name, price_override_reason, discount_type, haul_type, sale_upload_date, sale_upload_time))
    
    print(f"Message upserted into Cassandra for flight: {flight}")

# Close the Cassandra connection when done
cluster.shutdown()
