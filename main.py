from src.api_client import ProductAPIClient
"""
This script serves as the entry point for the product integration process. It utilizes the `ProductAPIClient` class 
from the `src.api_client` module to load, process, and send product data to an external API.

Workflow:
1. Initializes a `ProductAPIClient` instance with the path to the input CSV file containing raw product data.
2. Calls `load_and_process()` to load the data and perform necessary transformations.
3. Calls `create_batches()` to divide the processed data into manageable batches for API submission.
4. Calls `send_batches()` to send the batches to the external API.

Usage:
Run this script directly to execute the entire product integration pipeline.
"""
if __name__ == "__main__":
    client = ProductAPIClient("data/raw-bronze/items.csv")
    client.load_and_process()
    client.create_batches()
    client.send_batches()
