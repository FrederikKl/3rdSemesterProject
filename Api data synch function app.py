import azure.functions as func
import datetime
import json
import logging
import os
import json
import requests
import logging
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError


# %%
# Load environment variables from .env file
load_dotenv()

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 * * 6", arg_name="myTimer", run_on_startup=True, use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
        


    # Azure configuration
    AZURE_ACCOUNT_NAME = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
    AZURE_ACCOUNT_KEY = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
    AZURE_CONTAINER_NAME = os.getenv('AZURE_CONTAINER_NAME')
    API_STREAM = os.getenv('API_STREAM')

    # %%
    # Use account name and key
    blob_service_client = BlobServiceClient(
        account_url=f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net",
        credential=AZURE_ACCOUNT_KEY
    )


    # %%
    # Create a container if it doesn't already exist
    container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
    try:
        container_client.create_container()
    except ResourceExistsError:
        logging.info("Container already exists")

    # %%
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    logger = logging.getLogger()

    # %%
    def fetch_product_id_from_hjem_data_stream():
        """Fetch 'file_name' fields from the streamed data."""
        try:
            response = requests.get(API_STREAM, stream=True)  # Enable streaming
            response.raise_for_status()  # Raise an error for bad responses
            
            product_id = []  # To store the file names
            counter = 0

            # Process the response stream line by line
            for line in response.iter_lines():
                if line:  # Only process non-empty lines
                    # Parse the product JSON from each line
                    products = json.loads(line)  # Assuming the response is a list of products
                    if isinstance(products, list):
                        for product in products:
                            if 'id' in product:
                                product_id.append(product['id'])  # Add file_name to the list
                                logger.info(f"Fetched file_name for product #{counter + 1}: {product['id']}")
                                counter += 1
                    else:
                        if 'file_name' in products:
                            product_id.append(products['id'])  # Add file_name for a single product
                            logger.info(f"Fetched file_name for product #{counter + 1}: {products['id']}")
                            counter += 1

            logger.info(f"Total file names fetched: {counter}")
            return product_id  # Return the list of file names

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from API: {e}")
            return []  # Return an empty list in case of an error

    # %%
    def extract_ids_from_blobs():
        # List all blobs in the container
        blobs = container_client.list_blobs()

        # Initialize an empty list to store 'id' values
        ids = []

        # Loop through each blob and fetch its content
        for blob in blobs:
            # Get the blob client
            blob_client = container_client.get_blob_client(blob)
            
            # Download the blob's content
            blob_data = blob_client.download_blob().readall()
            
            # Try to parse the blob's content (assuming it's JSON)
            try:
                data = json.loads(blob_data)
                if 'id' in data:
                    ids.append(data['id'])
            except json.JSONDecodeError:
                print(f"Skipping blob {blob.name}, could not parse JSON.")

        return ids

    # %%
    def get_difference_between_product_and_blob_ids():
        """
        Fetches product IDs and blob IDs, then calculates the difference 
        between the two sets of IDs (i.e., the product IDs that are not in blob IDs).
        
        Returns:
            list: A list of IDs that are in product_ids but not in blob_ids.
        """
        # Fetch the product IDs and blob IDs
        product_ids = fetch_product_id_from_hjem_data_stream()
        blob_ids = extract_ids_from_blobs()
        
        # Calculate the difference between product_ids and blob_ids
        difference = list(set(product_ids) - set(blob_ids))
        
        return difference

    # %%
    def fetch_filtered_product_data_from_hjem_data_stream():
        """Fetch product data for product ids in the 'difference' list."""
        try:
            difference_in_products = get_difference_between_product_and_blob_ids()  # Call here

            response = requests.get(API_STREAM, stream=True)  # Enable streaming
            response.raise_for_status()  # Raise an error for bad responses
            
            product_data = []  # To store the filtered product data
            counter = 0
            
            # Process the response stream line by line
            for line in response.iter_lines():
                if line:  # Only process non-empty lines
                    # Parse the product JSON from each line
                    products = json.loads(line)  # Assuming the response is a list of products
                    if isinstance(products, list):
                        for product in products:
                            # Check if the product ID is in the 'difference' list
                            if product.get('id') in difference_in_products:
                                product_data.append(product)  # Add the product data to the list
                                logger.info(f"Fetched product data for product #{counter + 1} with ID {product.get('id')}")
                                counter += 1
                    else:
                        # Check if the single product ID is in the 'difference' list
                        if products.get('id') in difference_in_products:
                            product_data.append(products)  # Add the product data for the single product
                            logger.info(f"Fetched product data for product #{counter + 1} with ID {products.get('id')}")
                            counter += 1

            logger.info(f"Total filtered products fetched: {counter}")
            return product_data  # Return the list of filtered product data

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from API: {e}")
            return []  # Return an empty list in case of an error

    # Call the function without passing 'difference_in_products'


    # %%
    # Function to upload JSON objects from a list to Azure Blob Storage with custom blob names
    def upload_json_to_blob():
        # Create the BlobServiceClient object to connect to the storage account
        # Use account name and key
        blob_service_client = BlobServiceClient(    
        account_url=f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net",
        credential=AZURE_ACCOUNT_KEY
        )
        
        # Create the container if it doesn't already exist
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
        if not container_client.exists():
            container_client.create_container()

        products = fetch_filtered_product_data_from_hjem_data_stream()
        # Loop through the products list and upload each item as a separate blob
        for product in products:
            
            blob_name = f"{product['file_name'].replace('.txt', '.json')}"
            
            # Convert the product dictionary to a JSON string
            product_json = json.dumps(product,indent=9)
            
            # Get a blob client to interact with the specific blob (file)
            blob_client = container_client.get_blob_client(blob_name)
            
            # Upload the JSON data to the blob
            blob_client.upload_blob(product_json, blob_type="BlockBlob", overwrite=True)
            
            print(f"Uploaded: {blob_name}")


    # Call the function to upload the JSON objects
    upload_json_to_blob()


    # %%
    def extract_ids_from_blobs_for_deletion():
        # List all blobs in the container
        blobs = container_client.list_blobs()

        # Initialize an empty list to store 'id' values
        ids = []

        # Loop through each blob and fetch its content
        for blob in blobs:
            # Get the blob client
            blob_client = container_client.get_blob_client(blob)
            
            # Download the blob's content
            blob_data = blob_client.download_blob().readall()
            
            # Try to parse the blob's content (assuming it's JSON)
            try:
                data = json.loads(blob_data)
                if 'id' in data:
                    ids.append(data['id'])
            except json.JSONDecodeError:
                print(f"Skipping blob {blob.name}, could not parse JSON.")

        return ids

    # %%
    def fetch_product_id_from_hjem_data_stream_for_deletion():
        """Fetch 'file_name' fields from the streamed data."""
        try:
            response = requests.get(API_STREAM, stream=True)  # Enable streaming
            response.raise_for_status()  # Raise an error for bad responses
            
            product_id = []  # To store the file names
            counter = 0

            # Process the response stream line by line
            for line in response.iter_lines():
                if line:  # Only process non-empty lines
                    # Parse the product JSON from each line
                    products = json.loads(line)  # Assuming the response is a list of products
                    if isinstance(products, list):
                        for product in products:
                            if 'id' in product:
                                product_id.append(product['id'])  # Add file_name to the list
                                logger.info(f"Fetched file_name for product #{counter + 1}: {product['id']}")
                                counter += 1
                    else:
                        if 'file_name' in products:
                            product_id.append(products['id'])  # Add file_name for a single product
                            logger.info(f"Fetched file_name for product #{counter + 1}: {products['id']}")
                            counter += 1

            logger.info(f"Total file names fetched: {counter}")
            return product_id  # Return the list of file names

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from API: {e}")
            return []  # Return an empty list in case of an error

    # %%
    def get_difference_between_product_and_blob_ids_for_deletion():
        """
        Fetches product IDs and blob IDs, then calculates the difference 
        between the two sets of IDs (i.e., the product IDs that are not in blob IDs).
        
        Returns:
            list: A list of IDs that are in product_ids but not in blob_ids.
        """
        # Fetch the product IDs and blob IDs
        product_ids_for_deletion = fetch_product_id_from_hjem_data_stream_for_deletion()
        blob_ids_for_deletion = extract_ids_from_blobs_for_deletion()
        
        # Calculate the difference between product_ids and blob_ids
        difference_for_deletion = list(set(blob_ids_for_deletion) - set(product_ids_for_deletion))
        
        return difference_for_deletion

    # %%
    def delete_blobs_by_blobs():
        blob_service_client = BlobServiceClient(    
        account_url=f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net",
        credential=AZURE_ACCOUNT_KEY
        )
        
        deletion_ids = get_difference_between_product_and_blob_ids_for_deletion()
        
        # Create the container if it doesn't already exist
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
        for blob in container_client.list_blobs():
            blob_name = blob.name
            # Get a blob client to interact with the specific blob
            blob_client = container_client.get_blob_client(blob_name)

            # Download the blob's content (assuming it's a JSON file)
            try:
                # Download blob data and read it
                blob_data = blob_client.download_blob().readall()
                # Parse the content as JSON
                product_data = json.loads(blob_data)

                # Check if 'id' exists and if it matches any of the deletion_ids
                if 'id' in product_data and product_data['id'] in deletion_ids:
                    print(f"Deleting blob: {blob_name} (ID: {product_data['id']})")
                    # Delete the blob if the ID matches
                    blob_client.delete_blob()
            except Exception as e:
                print(f"Error processing blob {blob_name}: {e}")
    delete_blobs_by_blobs()

    # %%
    def main():
        # Set up logging
        upload_blob = upload_json_to_blob()
        
        delete = delete_blobs_by_blobs()

        logging.info("Main function executed successfully.")


        
        
    # Place your existing functions below this line...
    # (e.g., fetch_product_id_from_hjem_data_stream, extract_ids_from_blobs, etc.)

    if __name__ == "__main__":
        main()



    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
    

