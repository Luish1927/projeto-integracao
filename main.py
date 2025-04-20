import pandas as pd
import re
import numpy as np
from unidecode import unidecode
import requests
import json
import logging
from dotenv import load_dotenv
import os


logging.basicConfig(filename='processing.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')



def float_to_string(barcode):
    """
    Function that converts the barcode from float to string
    In case of null values, it returns an empty list

    :param barcode: float - The barcode to be converted.
    :return: list [string] - A list containing the converted barcode.
    """
    if pd.notnull(barcode):
        return [str(int(barcode))]
    return []


def format_to_ean(barcode):
    """
    Function that formats the barcode to EAN-8, EAN-12, or EAN-13
    In case of invalid barcodes, it returns an empty list to avoid errors

    :param barcode: list[string] - The barcode to be formatted.
    :return: list [string] - A list containing the formatted barcode.
    """
    barcode = str(barcode)
    ean8_regex = r'^\d{8}$'   
    ean12_regex = r'^\d{12}$'  
    ean13_regex = r'^\d{13}$'  
    
    if re.match(ean8_regex, barcode):
        return [barcode]
    elif re.match(ean12_regex, barcode):
        return [barcode]
    elif re.match(ean13_regex, barcode):
        return [barcode]
    else:
        return []


def promo_price_formater(price):
    """
    Function that formats the promotional price to float
    In case of null values or invalid prices, it returns None
    It swaps the comma for a dot to correspond to the API format

    :param price: - The promotional price to be formatted.
    :return: float - The formatted promotional price.
    """
    if pd.notnull(price) and price != '0':
        return pd.to_numeric(price.replace(',', '.'), errors='coerce')
    return None


def price_formater(price):
    """
    Function that formats the regular price to float and rounds it to 2 decimal places
    It swaps the comma for a dot to correspond to the API format

    :param price: - The price to be formatted.
    :return: float - The formatted price.
    """
    if pd.notnull(price):
        return round(pd.to_numeric(price.replace(',', '.'), errors='coerce'), 2)


def internal_code_formater(code):
    """
    Function that formats the internal code to string
    Since there is no null values there is no need to drop any row

    :param code: - The internal code to be formatted.
    :return: str - The formatted internal code.
    """
    if pd.notnull(code):
        return str(code)


def stock_formater(price):
    """
    Function that formats the stock to float

    :param price: - The stock value to be formatted.
    :return: float - The formatted stock value.
    """
    if pd.notnull(price):
        return float(price)
    else: 
        return None


def measure_parser(name):
    """
    This function is responsible for parsing the product's name to extract its dimensions and weight if available.
    It uses regex to identify patterns such as weight --weight_match (kg/g) and dimensions --dimension_match (cm/m), like the following examples respectively, 100g and 30cm x 20 cm x 10cm. It only considers as a measure if the product name contains the unit of measure.

    It will return a dictionary with the following keys: length, width, height, and weight, all set to None if not found and taking into consideration the proper conversion of units.
    It swaps the comma for a dot to correspond to the API format.

    :param name: str - The name of the product to be parsed.
    :return: dict - A dictionary containing the extracted measures.
    """
    normalized_name = unidecode(str(name).lower())
    measures = {'length': None, 'width': None, 'height': None, 'weight': None}

    weight_match = re.search(r'(?<!\d)(\d+(?:[\.,]\d+)?)\s*(kg|g)\b', normalized_name)
    if weight_match:
        value, unit = weight_match.groups()
        value = float(value.replace(',', '.'))
        if unit == 'g':
            value /= 1000
        measures['weight'] = round(value, 3)

    dimension_match = re.search(
        r'(\d+(?:[\.,]\d+)?)\s*(cm|m)\s*[/xX]\s*(\d+(?:[\.,]\d+)?)\s*(cm|m)(?:\s*[/xX]\s*(\d+(?:[\.,]\d+)?)\s*(cm|m))?',
        normalized_name
    )
    if dimension_match:
        g = dimension_match.groups()
        measures['length'] = float(g[0].replace(',', '.'))
        measures['width'] = float(g[2].replace(',', '.'))
        if g[4]:
            measures['height'] = float(g[4].replace(',', '.'))

    return measures


def infer_unit_type(name):
    """
    This function is responsible for parsing the product's name to infer the unit type. It uses regex to identify patterns such as 'uni, unidade' or numbers followed by 'ml, l, kg, g' to return the unit_type as 'UNI'. For times when the product's name contains 'kg, g' it returns 'KG'.
    The last case considers the product's name with more than 4 words as 'UNI' to avoid false positives.

    :param name: str - The name of the product to be parsed.
    :return: str - The inferred unit type ('UNI' or 'KG').
    """
    clean_name = unidecode(str(name).lower())
    words = clean_name.split()

    if re.search(r'\b(un|unid|unidade|pct|pacote|cx|caixa|frasco|galao)\b', clean_name):
        return "UNI"

    if re.search(r'\d+(ml|l)\b', clean_name):
        return "UNI"

    if re.search(r'\d+(kg|g)\b', clean_name):
        return "UNI"

    if re.search(r'\b(kg|g)\b', clean_name):
        return "KG"

    if not (re.search(r'\b(kg|g)\b', clean_name)) or len(words) >= 4:
        return "UNI"

    return "KG"


def process_csv_to_batches(input_path):
    """
    This function processes a CSV file containing product data, extracts and formats relevant information, 
    and organizes the data into batches of 1000 products for further API submission.

    :param input_path: str - The file path to the input CSV file.
    :return: list - A list of dictionaries, where each dictionary represents a batch of products.
    """
    logging.info("Starting process_csv_to_batches function.")
    df = pd.read_csv(input_path, sep=';')

    unit_types = []
    weights = []
    lengths = []
    widths = []
    heights = []

    for name in df['Nome']:
        unit_type = infer_unit_type(name)
        measures = measure_parser(name)

        unit_types.append(unit_type)
        weights.append(measures['weight'])
        lengths.append(measures['length'])
        widths.append(measures['width'])
        heights.append(measures['height'])

    # Add the extracted data as new columns in the DataFrame
    df['unit_type'] = unit_types
    df['weight'] = weights
    df['length'] = lengths
    df['width'] = widths
    df['height'] = heights

    # Format and clean other columns in the DataFrame
    df['name'] = df['Nome'].apply(lambda x: str(x))  
    df['barcodes'] = df['Código de barras'].apply(float_to_string)  
    df['barcodes'] = df['barcodes'].apply(lambda x: format_to_ean(x[0]) if x else [])  
    df['promo_price'] = df['Promocao'].apply(promo_price_formater) 
    df['price'] = df['Preço regular'].apply(price_formater) 
    df['visible'] = df['ativo']  
    df['internal_code'] = df['Código interno'].apply(internal_code_formater)  
    df['stock'] = df['estoque'].apply(stock_formater) 
    
    df['promo_end_at'] = df["Data termino promocao"].apply(lambda x: "2020-03-31T23:59:59.672000-03:00")
    df['promo_start_at'] = df["Data termino promocao"].apply(lambda x: "2020-03-31T23:59:59.672000-03:00")

    
    source_columns = ['Nome', 'Código de barras', 'Promocao', 'Preço regular', 'ativo', 'Código interno', 'estoque', 'Data termino promocao']
    df.drop(columns=source_columns, inplace=True)
    new_colums = ['internal_code', 'name', 'unit_type', 'price', 'visible', 'stock', 'barcodes', 'promo_price', 'weight', 'length', 'width', 'height', 'promo_end_at', 'promo_start_at']
    df = df[new_colums]

    
    df.replace({np.nan: None}, inplace=True)

    
    batches = []
    output_dir = 'batches'
    os.makedirs(output_dir, exist_ok=True)  
    
    for i in range(0, len(df), 1000):
        batch = {"products": df[i:i+1000].apply(lambda row: row.to_dict(), axis=1).tolist()}
        batches.append(batch)
        with open(f'{output_dir}/batch_{i//1000 + 1}.json', 'w', encoding='utf-8') as json_file:
            json.dump(batch, json_file, ensure_ascii=False, indent=4)

    logging.info("Finished process_csv_to_batches function.")
    return batches


def send_to_api(batches):
    """
    This function sends batches of product data to an external API for processing.

    :param batches: list - A list of dictionaries, where each dictionary represents a batch of products to be sent.
    """
    logging.info("Starting send_to_api function.")
    load_dotenv() 

   
    url = 'https://api.instabuy.com.br/store/products'
    api_key = os.getenv('API_KEY')  

    
    headers = {
        'Content-Type': 'application/json',
        'api-key': api_key
    }

    for i, batch in enumerate(batches, start=1):
        try:
            response = requests.put(url, headers=headers, data=json.dumps(batch))

            if response.status_code == 200:
                print(response.json())  
                logging.info(f'Batch {i} sent succesfully.')  
            else:
                logging.error(f'Batch {i}: Error while sending - {response.text}')
        except Exception as e:
            logging.error(f'Batch {i}: Exception while sending - {str(e)}')

    logging.info("Finished send_to_api function.")

batches = process_csv_to_batches("raw-bronze/items.csv")
send_to_api(batches)

