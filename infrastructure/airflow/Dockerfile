# Use an official Airflow image as a parent image
FROM apache/airflow:latest

# Copy the requirements file and install Python dependencies
COPY . .

# Install Poetry & package dependencies
RUN pip install poetry
RUN poetry install

# Switch back to airflow user for safety
USER airflow