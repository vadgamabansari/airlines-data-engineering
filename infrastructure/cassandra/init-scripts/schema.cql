CREATE KEYSPACE IF NOT EXISTS data_engineering_airline
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };

USE data_engineering_airline;

-- Create database table PAX Data
CREATE TABLE IF NOT EXISTS pax (
    flight_date DATE,                                       -- The date of the flight in the format YYYY-MM-DD
    airline VARCHAR,                                        -- The airline code 
    flight_number INT,                                      -- The unique flight number (e.g., 5750)
    boarded_y INT,                                          -- The number of passengers who actually boarded the flight
    capacity_physical_y INT,                                -- The physical capacity of the aircraft (total seats available for the flight)
    capacity_saleable_y INT,                                -- The saleable capacity of the flight (number of seats available for sale)
    PRIMARY KEY ((flight_date, airline), flight_number)     -- Composite primary key using flight_date and airline, with flight_number as a clustering column
);

-- Create database table Loading Data
CREATE TABLE IF NOT EXISTS loading (
    flight_month_year INT,                                  -- Original field indicating month and year of the flight (format MMYYYY or MYYYY)
    flight_month INT,                                       -- Extracted month from the flight_month_year field (1 or 2 digits)
    flight_year INT,                                        -- Extracted year from the flight_month_year field (4 digits)
    airline VARCHAR,                                        -- Airline code 
    flight_number INT,                                      -- Unique flight number identifier (e.g., 5750)
    departure_unit VARCHAR,                                 -- Departure airport code (e.g., FRA for Frankfurt)
    arrival_unit VARCHAR,                                   -- Arrival airport code (e.g., HKG for Hong Kong)
    flight_date DATE,                                       -- The date on which the flight takes place
    aircraft_type INT,                                      -- Type of aircraft used for the flight (e.g., 319 for Airbus A319)
    leg INT,                                                -- Flight leg (1 for single-leg flights, 2 or higher for multi-leg flights)
    booking_class_cbase VARCHAR,                            -- Booking class code (e.g., CC for economy)
    bill_of_material VARCHAR,                               -- Bill of material, which may represent a booking or operational code (e.g., 1/M/456926)
    price FLOAT,                                            -- Price of the service or ticket for the flight
    invoice_quantity INT,                                   -- Number of items or quantity billed for the invoice (e.g., number of tickets sold)
    sales_services_invoice VARCHAR,                         -- Original fields for invoice amounts and currencies
    sales_services_invoice_value FLOAT,                     -- Original field for sales and services invoice (e.g., '9,10 EUR')
    sales_services_invoice_currency VARCHAR,                -- Extracted currency from the sales_and_services_invoice field (e.g., 'EUR')
    uaa_invoice VARCHAR,                                    -- Original field for UAA invoice (e.g., '0,32 EUR')
    uaa_invoice_value FLOAT,                                -- Extracted numeric value from the uaa_invoice field (e.g., 0.32)
    uaa_invoice_currency VARCHAR,                           -- Extracted currency from the uaa_invoice field (e.g., 'EUR')
    PRIMARY KEY ((flight_date, airline, flight_number), leg, booking_class_cbase, bill_of_material) -- Composite primary key to identify a unique record
);


-- Create database table Sales Data
-- NOTE: TODO - Work in Progress (Non Functional/Not Tested)
CREATE TABLE IF NOT EXISTS sales (
    rank INT,                                           -- Rank of the sale (used for ordering or ranking purposes)
    transaction_id VARCHAR,                             -- Unique identifier for the transaction (primary key)
    origin VARCHAR,                                     -- Origin airport code (e.g., FRA for Frankfurt)
    destination VARCHAR,                                -- Destination airport code (e.g., JFK for New York)
    flight VARCHAR,                                     -- Flight number for the transaction
    flight_date DATETIME,                               -- Date and time of the flight (includes both date and time)
    short_flight_date DATE,                             -- Date of the flight (without time)
    flight_date_time TIME,                              -- Time of the flight (without date)
    transaction_date DATETIME,                          -- Date and time when the transaction occurred (includes both date and time)
    short_transaction_date DATE,                        -- Date when the transaction occurred (without time)
    transaction_date_time TIME,                         -- Time when the transaction occurred (without date)
    aircraft_registration VARCHAR,                      -- Registration number of the aircraft
    aircraft_type VARCHAR,                              -- Type of aircraft used for the flight
    crew_member VARCHAR,                                -- Identifier of the crew member associated with the sale
    product_type VARCHAR,                               -- Type of product sold (e.g., food, beverage, duty-free item)
    product_code VARCHAR,                               -- Product code for the item sold
    quantity INT,                                       -- Quantity of the item sold
    cost_price INT,                                     -- Cost price of the product (in base currency)
    net_sales INT,                                      -- Net sales amount (excluding VAT and other taxes)
    vat INT,                                            -- VAT (Value-Added Tax) applied to the sale
    gross_sales INT,                                    -- Gross sales amount (including VAT and other taxes)
    total_sales DECIMAL(10,2),                          -- Total sales amount (with higher precision for monetary values)
    cash VARCHAR,                                       -- Cash amount paid for the transaction
    card VARCHAR,                                       -- Card payment details for the transaction (can include card type or masked card number)
    voucher VARCHAR,                                    -- Voucher details if the transaction was paid with a voucher
    payment_details VARCHAR,                            -- Additional payment details (may include multiple payment methods)
    reason VARCHAR,                                     -- Reason for the transaction or sale (if any special conditions apply)
    voided VARCHAR,                                     -- Indicates if the transaction was voided (yes/no)
    device_code BIGINT,                                 -- Unique identifier for the device used to process the transaction
    payment_status VARCHAR,                             -- Status of the payment (e.g., completed, pending, refunded)
    base_currency_price DECIMAL(10,2),                  -- Price of the product in the base currency of the flight
    sale_item_id BIGINT,                                -- Unique identifier for the sale item
    sale_item_status INT,                               -- Status of the sale item (e.g., delivered, pending, canceled)
    vat_rate_value INT,                                 -- VAT rate applied to the sale item
    crew_base_code VARCHAR,                             -- Base code of the crew responsible for the sale
    refund_reason VARCHAR,                              -- Reason for refund (if applicable)
    payment_id BIGINT,                                  -- Unique identifier for the payment (if multiple payments are tracked)
    price_type VARCHAR,                                 -- Type of pricing applied to the sale (e.g., regular, discount, override)
    seat_number VARCHAR,                                -- Seat number of the passenger who made the purchase
    passenger_count INT,                                -- Number of passengers associated with the transaction
    schedule_actual_departure_time TIME,                -- Scheduled or actual departure time of the flight
    schedule_actual_arrival_time TIME,                  -- Scheduled or actual arrival time of the flight
    sale_type_name VARCHAR,                             -- Type of sale (e.g., pre-flight, in-flight, post-flight)
    price_override_reason VARCHAR,                      -- Reason for overriding the original price (if applicable)
    discount_type VARCHAR,                              -- Type of discount applied (e.g., loyalty, staff discount)
    haul_type VARCHAR,                                  -- Type of flight haul (e.g., short-haul, long-haul)
    sale_upload_date DATE,                              -- Date when the sale record was uploaded to the system
    sale_upload_time TIME,                              -- Time when the sale record was uploaded to the system
    PRIMARY KEY (transaction_id, flight_date)           -- Composite primary key (transaction_id as partition key, flight_date as clustering key)
);


-- (Data Science) Create table for data science use cases
CREATE TABLE IF NOT EXISTS fact_flight_analysis (
    flight_date DATE,                                       -- The date of the flight
    airline VARCHAR,                                        -- The airline code 
    flight_number INT,                                      -- The unique flight number
    boarded_y INT,                                          -- The number of passengers who boarded the flight
    capacity_physical_y INT,                                -- The physical capacity of the aircraft
    capacity_saleable_y INT,                                -- The saleable capacity of the flight
    departure_unit VARCHAR,                                 -- The airport code for departure
    arrival_unit VARCHAR,                                   -- The airport code for arrival
    aircraft_type INT,                                      -- The type of aircraft used
    leg INT,                                                -- The leg of the flight
    price FLOAT,                                            -- The price of the ticket for the flight
    booking_class_cbase VARCHAR,                            -- The booking class for the flight
    bill_of_material VARCHAR,                               -- Bill of material for the flight
    PRIMARY KEY ((flight_date, airline), flight_number, leg)
);
