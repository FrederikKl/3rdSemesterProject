import os
import json
import psycopg2
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
import azure.functions as func
import logging
import re # Regular expression library for parsing strings
from dotenv import load_dotenv
load_dotenv()
    
app = func.FunctionApp()
    
# Database connection parameters (use environment variables for security)
@app.timer_trigger(schedule="0 0 8 * * 6", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def weekly_data_sync(myTimer: func.TimerRequest) -> None:

        
    # Configuration: Define your parameters here
    time_range = "last_week"  # Options: "last_week" or "last_two_months"
    difference_threshold = 0.81  # Only include records with a difference <= this value
    order_by_column = "added_at"  # Column to sort by
    sort_direction = "DESC"  # Sorting direction (ASC or DESC) 
    
    # Determine the date interval based on time_range
    if time_range == "last_week":
        date_start = datetime.now() - timedelta(weeks=4)
    elif time_range == "last_two_months": # Default to last two months, can be changed to a different range
        date_start = datetime.now() - timedelta(weeks=8)
    else:
        raise ValueError("Invalid time range specified.")
    
    # Database connection parameters (use environment variables for security)
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')    # Azure Blob Storage connection parameters
    blob_connection_string = os.getenv('AZURE_CONNECTION_STRING')
    container_name =  os.getenv('AZURE_CONTAINER_NAME')  # The folder/container to upload to in Blob Storage
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        )
        print("Connected to PostgreSQL")
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return  # Exit the function if connection fails     

    try:
        with conn.cursor() as cursor:

            # Parameterized query to fetch records from the database 
            query = f"""
                SELECT added_at, difference, id, prefix, user_request, original, edited
                FROM corrections
                WHERE added_at >= %s
                AND difference <= %s
                ORDER BY {order_by_column} {sort_direction};
            """
            cursor.execute(query, (date_start, difference_threshold))
            records = cursor.fetchall()
            print(f"Fetched {len(records)} records from the database.")
            
            # Initialize Blob Service Client
            blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
            print("Blob service client initialized.")
        
            # Initialize the container client
            container_client = blob_service_client.get_container_client(container_name)
            
            # Determine the current counter based on existing blobs
            blobs_metadata = {blob.name: blob for blob in container_client.list_blobs()}
            example_files = [name for name in blobs_metadata if name.startswith("example") and name.endswith(".txt")]

            # Extract the numbers from existing files to find the highest one
            highest_number = 10  # Starting from 11 if no example files are found
            for name in example_files:
                try:
                    # Use regex to extract the number after "example" and before ".txt"
                    match = re.search(r"example(\d+)\.txt$", name) # re capturing a sequence of digits as a group (\d+)
                    if match:
                        number = int(match.group(1))  # Extract the number as an integer
                        highest_number = max(highest_number, number)
                    else:
                        print(f"Error parsing number from {name}: could not extract a valid number")
                except ValueError as e:
                    print(f"Error parsing number from {name}: {e}")

            # Set the counter to start from the next available number
            counter = highest_number + 1
            print(f"Starting counter based on existing blobs: {counter}")

            # Process each record individually
            for record in records:
                
                try: 
                    # Format each record into the specified JSON structure
                    formatted_record = {
                        "meta": "This is an example of a question that a customer has had, and the reply made by the company support agent.",
                        "customer_request": record[4], # user_request
                        "company_reply": record[6], # edited
                        "type": "example"
                    }

                    # Convert the single record to JSON format
                    json_data = json.dumps(formatted_record, indent=4)

                    # Define a unique filename using the record's id and timestamp
                    filename =  f"examples/example{counter}.txt"
                    print(f"Uploading {filename}...")

                    # upload the json data as .txt file for each record to Azure Blob Storage
                    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
                    blob_client.upload_blob(json_data, overwrite=False)               
                    print(f"Uploaded {filename} successfully.")
                    
                    # Increment the counter for the next file
                    counter += 1

                except Exception as e:
                    print(f"Failed to upload {filename}: {e}")
                    
        if myTimer.past_due:
            logging.info('The timer is past due!')

        logging.info('Python timer trigger function executed.')
    finally:
        conn.close()
