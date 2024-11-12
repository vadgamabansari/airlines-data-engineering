## Data Structures

This section describes about our data that we have built our project on.

#### Entities:
- **PAX:** Flight Passenger Information
- **LOADING:** Information about items loaded on an aircraft
- **SALES:** In-flight sales done by Airline staff members

### Sample and Input Data directories
---
This directory contains two main folders described below
- **ingest:** Used to track all incoming files that has to be processed by Apache Airflow DAGs.
- **sample:** This directory tracks all the sample files that we have to leverage in our use-case project.

### Data Dictionary / Description
---
#### 1. PAX:
The PAX table in this project stores passenger-related data for each flight. It is a key component in building the flight analysis fact table, as it captures details about the number of passengers who boarded a flight, the physical capacity of the aircraft, and the saleable capacity. The PAX data is used to analyze the relationship between passenger load, aircraft capacity, and operational efficiency.

#### Data Descriptions:
- **flight_date:** The date of the flight in YYYY-MM-DD format.
- **airline:** Code representing the airline.
- **flight_number:** Unique flight number for each flight.
- **boarded_y:** Number of passengers who boarded the flight.
- **capacity_physical_y:** The physical capacity of the aircraft (total seats available).
- **capacity_saleable_y:** Number of seats available for sale.

#### 2. Loading Table:
The Loading table captures detailed operational data about each flight, including the departure and arrival airports, aircraft type, flight legs, and pricing information. It stores valuable information like the bill of material (bill_of_material) and the booking class (booking_class_cbase), making it useful for understanding the logistical and financial aspects of flight operations. This table plays a key role in analyzing the operational efficiency of flights and helps in pricing and load distribution analysis across different airlines and aircraft.

#### Data Descriptions:
- **flight_month_year:** The month and year of the flight in MMYYYY or MYYYY format.
- **flight_month:** The extracted month from flight_month_year.
- **flight_year:** The extracted year from flight_month_year.
- **airline:** Airline code.
- **flight_number:** Unique flight number for each flight.
- **departure_unit:** Departure airport code (e.g., FRA for Frankfurt).
- **arrival_unit:** Arrival airport code (e.g., HKG for Hong Kong).
- **flight_date:** The date of the flight.
- **aircraft_type:** Type of aircraft used for the flight.
- **leg:** Flight leg number (e.g., 1 for single-leg flights).
- **booking_class_cbase:** Booking class for the flight.
- **bill_of_material:** Bill of material for the flight.
- **price:** Price of the flight ticket.
- **invoice_quantity:** Quantity billed for the invoice.
- **sales_services_invoice:** Details of the sales and services invoice.
- **sales_services_invoice_value:** Numeric value of the sales and services invoice.
- **sales_services_invoice_currency:** Currency used in the sales and services invoice.
- **uaa_invoice:** UAA invoice details.
- **uaa_invoice_value:** Numeric value of the UAA invoice.
- **uaa_invoice_currency:** Currency used in the UAA invoice.

#### 3. Sales:
The Sales table tracks in-flight sales transactions, detailing the products sold, payment methods used, and related crew members. It stores information such as the transaction_id, product_type, and payment_status, allowing for analysis of sales performance and payment trends during flights. It also captures timestamps related to when the sales occurred and the flight details, making it useful for understanding how sales vary across different flights and time periods. This table provides insights into revenue generation from in-flight services.

#### Data Description:
- **rank:** Rank of the sale for ordering purposes.
- **transaction_id:** Unique identifier for the transaction.
- **origin:** Origin airport code.
- **destination:** Destination airport code.
- **flight:** Flight number for the transaction.
- **flight_date:** Date and time of the flight.
- **short_flight_date:** Date of the flight (without time).
- **flight_date_time:** Time of the flight (without date).
- **transaction_date:** Date and time of the transaction.
- **short_transaction_date:** Date of the transaction (without time).
- **transaction_date_time:** Time of the transaction (without date).
- **aircraft_registration:** Registration number of the aircraft.
- **aircraft_type:** Type of aircraft used.
- **crew_member:** Identifier for the crew member responsible for the sale.
- **product_type:** Type of product sold.
- **product_code:** Code representing the product.
- **quantity:** Quantity of the product sold.
- **cost_price:** Cost price of the product.
- **net_sales:** Net sales amount (excluding VAT).
- **vat:** VAT applied to the sale.
- **gross_sales:** Gross sales amount (including VAT).
- **total_sales:** Total sales amount.
- **cash:** Cash amount paid.
- **card:** Card payment details.
- **voucher:** Voucher details if applicable.
- **payment_details:** Additional payment details.
- **reason:** Reason for the transaction.
- **voided:** Indicates if the transaction was voided.
- **device_code:** Unique identifier for the device used for the transaction.
- **payment_status:** Status of the payment (e.g., completed).
- **base_currency_price:** Price of the product in the base currency.
- **sale_item_id:** Unique identifier for the sale item.
- **sale_item_status:** Status of the sale item.
- **vat_rate_value:** VAT rate applied.
- **crew_base_code:** Base code of the crew member.
- **refund_reason:** Reason for a refund (if applicable).
- **payment_id:** Unique identifier for the payment.
- **price_type:** Type of pricing applied.
- **seat_number:** Seat number of the passenger.
- **passenger_count:** Number of passengers involved in the transaction.
- **schedule_actual_departure_time:** Scheduled or actual departure time.
- **schedule_actual_arrival_time:** Scheduled or actual arrival time.
- **sale_type_name:** Type of sale.
- **price_override_reason:** Reason for price override.
- **discount_type:** Type of discount applied.
- **haul_type:** Type of flight haul (e.g., short-haul, long-haul).
- **sale_upload_date:** Date when the sale record was uploaded.
- **sale_upload_time:** Time when the sale record was uploaded.

#### 4. Flight Analysis Fact Table:

The Fact Flight Analysis table integrates data from both the PAX and Loading tables to offer a holistic view of flight performance. It includes key metrics like the number of passengers who boarded, the aircraft's capacity, ticket prices, and flight legs. By combining passenger and operational data, this table enables comprehensive data science analysis that can be used to assess flight profitability, passenger load, and operational efficiency. It is ideal for exploring correlations between ticket prices, capacity utilization, and other flight characteristics for better business decision-making.

#### Data Description:
- **flight_date:** Date of the flight.
- **airline:** Airline code for the flight.
- **flight_number:** Flight number for the flight.
- **boarded_y:** Number of passengers who boarded the flight.
- **capacity_physical_y:** Physical capacity of the aircraft.
- **capacity_saleable_y:** Number of seats available for sale.
- **departure_unit:** Departure airport code.
- **arrival_unit:** Arrival airport code.
- **aircraft_type:** Type of aircraft used.
- **leg:** Flight leg.
- **price:** Price of the flight ticket.
- **booking_class_cbase:** Booking class for the flight.
- **bill_of_material:** Bill of material for the flight.