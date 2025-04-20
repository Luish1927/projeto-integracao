import re
from unidecode import unidecode
import pandas as pd
from datetime import datetime

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
    
def ptbr_to_iso_format(date_str):
    """
    This function is responsible for formating the date string from the "Data termino promocao" column from the format "31-MAR-24" to the ISO format "2024-03-31T00:00:00".
    It uses regex to match the date format and convert it to the desired format.\
    
    :param date_str: The date string in the format "31-MAR-24"
    :return: The date string in the ISO format "2024-03-31T00:00:00" or None if the input is invalid.
    """
    months_pt = {
        'JAN': '01', 'FEV': '02', 'MAR': '03', 'ABR': '04', 'MAI': '05', 'JUN': '06',
        'JUL': '07', 'AGO': '08', 'SET': '09', 'OUT': '10', 'NOV': '11', 'DEZ': '12'
    }

    date_regex = re.compile(r'(\d{1,2})-([A-Z]{3})-(\d{2,4})', re.IGNORECASE)

    match = date_regex.match(date_str.upper())
    if not match:
        return None
    day, month_abbr, year = match.groups()
    month = months_pt.get(month_abbr)
    if not month:
        return None
    if len(year) == 2:
        year = '20' + year
    try:
        return f"{year}-{month}-{int(day):02d}T00:00:00"
    except:
        return None
    
def process_promo_dates(date):
    """
    This function processes the 'Data termino promocao' column in a DataFrame to extract and format promotional start 
    and end dates. It handles different date formats and converts them into ISO 8601 format.

    - For date ranges, the function splits the string into start and end dates and converts them to ISO format.
    - For single dates, it assumes the date is the end date and converts it to ISO format.
      convert it to ISO format.
    - Invalid or missing dates are handled gracefully and set to None.
    
    :param df: pandas.DataFrame - The input DataFrame containing a column named 'Data termino promocao'.
    :return: pandas.DataFrame - The modified DataFrame with two new columns: 'promo_start_at' and 'promo_end_at'.
    """
    if pd.isnull(date) or str(date).lower() == 'nan':
        return None, None

    if '/' in date:
        date_segments = date.split('/')
        try:
            promo_start = datetime.fromisoformat(date_segments[0]).isoformat()
        except:
            promo_start = None
        try:
            promo_end = datetime.fromisoformat(date_segments[1]).isoformat()
        except:
            promo_end = None
    else:
        promo_start = None
        promo_end = ptbr_to_iso_format(date)

    return promo_start, promo_end