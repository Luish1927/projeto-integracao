import os
import json
import requests
import logging
from dotenv import load_dotenv

from .processor import ProductDataProcessor

logging.basicConfig(filename='processing.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class ProductAPIClient(ProductDataProcessor):
    def __init__(self, input_path: str):
        super().__init__(input_path)
        load_dotenv()
        self.api_key = os.getenv('API_KEY')
        self.url = 'https://api.instabuy.com.br/store/products'

    def send_batches(self):
        logging.info("Starting send_batches")

        if not self.batches:
            raise ValueError("No batches to send. Did you forget to call load_and_process and create_batches?")

        headers = {
            'Content-Type': 'application/json',
            'api-key': self.api_key
        }

        for i, batch in enumerate(self.batches, start=1):
            try:
                response = requests.put(self.url, headers=headers, data=json.dumps(batch))

                if response.status_code == 200:
                    print(response.json())
                    logging.info(f'Batch {i} sent successfully.')
                else:
                    logging.error(f'Batch {i}: Error while sending - {response.text}')
            except Exception as e:
                logging.error(f'Batch {i}: Exception while sending - {str(e)}')

        logging.info("Finished send_batches")
