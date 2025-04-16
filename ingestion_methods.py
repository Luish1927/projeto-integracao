import pandas as pd
import re
import numpy as np

# converts float to string
def float_to_string(barcode):
    if pd.notnull(barcode):
        return [str(int(barcode))]
    return []

def format_to_ean(barcode):

    barcode = str(barcode)

    # regex to validate EAN-8, EAN-12 and EAN-13
    ean8_regex = r'^\d{8}$'   # 8 numerical digits
    ean12_regex = r'^\d{12}$'  # 12 numerical digits
    ean13_regex = r'^\d{13}$'  # 13 numerical digits
    
    if re.match(ean8_regex, barcode):
        return [barcode]
    elif re.match(ean12_regex, barcode):
        return [barcode]
    elif re.match(ean13_regex, barcode):
        return [barcode]
    else:
        return []
    
# formats the promo price to float and swap comma to dot
def promo_price_formater(price):
    if pd.notnull(price) and price != '0':
        return pd.to_numeric(price.replace(',', '.'), errors='coerce')
    return np.nan

# formats the price to float and swap comma to dot
def price_formater(price):
    if pd.notnull(price):
        return round(pd.to_numeric(price.replace(',', '.'), errors='coerce'), 2)
    
# formats the internal code to string
def internal_code_formater(code):
    if pd.notnull(code):
        return str(code)
    
# formats the stock to float
def stock_formater(price):
    if pd.notnull(price):
        return float(price)     
    
    
df = pd.read_csv('items.csv', sep=';')
df_treated = pd.DataFrame()

df_treated['name'] = df['Nome'].apply(lambda x: str(x))
df_treated['barcodes'] = df['Código de barras'].apply(float_to_string)
df_treated['barcodes'] = df_treated['barcodes'].apply(lambda x: format_to_ean(x[0]) if x else [])
df_treated['promo_price'] = df['Promocao'].apply(promo_price_formater)
df_treated['price'] = df['Preço regular'].apply(price_formater)
df_treated['visible'] = df['ativo']
df_treated['internal_code'] = df['Código interno'].apply(internal_code_formater)
df_treated['stock'] = df['estoque'].apply(stock_formater)


df_treated.to_csv('cols_validation.csv', index=False)


