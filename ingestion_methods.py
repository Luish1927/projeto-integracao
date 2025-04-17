import pandas as pd
import re
import numpy as np
from unidecode import unidecode


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

def parse_medidas(nome):
    nome_limpo = unidecode(str(nome).lower())
    medidas = {'length': None, 'width': None, 'height': None, 'weight': None}

    # Peso - somente se unidade for kg ou g (nunca ml ou l)
    peso_match = re.search(r'(?<!\d)(\d+(?:[\.,]\d+)?)\s*(kg|g)\b', nome_limpo)
    if peso_match:
        valor, unidade = peso_match.groups()
        valor = float(valor.replace(',', '.'))
        if unidade == 'g':
            valor /= 1000
        medidas['weight'] = round(valor, 3)

    # Medidas físicas - só se tiver unidade de medida explícita (cm, m, etc.)
    comp_match = re.search(
        r'(\d+(?:[\.,]\d+)?)\s*(cm|m)\s*[/xX]\s*(\d+(?:[\.,]\d+)?)\s*(cm|m)(?:\s*[/xX]\s*(\d+(?:[\.,]\d+)?)\s*(cm|m))?',
        nome_limpo
    )
    if comp_match:
        g = comp_match.groups()
        medidas['length'] = float(g[0].replace(',', '.'))
        medidas['width'] = float(g[2].replace(',', '.'))
        if g[4]:
            medidas['height'] = float(g[4].replace(',', '.'))

    return medidas

def inferir_unit_type(nome):
    nome_limpo = unidecode(str(nome).lower())
    palavras = nome_limpo.split()

    # Regra 1: Unidades explícitas no nome
    if re.search(r'\b(un|unid|unidade|pct|pacote|cx|caixa|frasco|galao)\b', nome_limpo):
        return "UNI"

    # Regra 2: Volume → trata como UNI
    if re.search(r'\d+(ml|l)\b', nome_limpo):
        return "UNI"

    # Regra 3: Peso explícito → trata como UNI (ex: 1kg)
    if re.search(r'\d+(kg|g)\b', nome_limpo):
        return "UNI"

    # Regra 4: Somente a unidade 'kg' ou 'g' → trata como vendido a peso
    if re.search(r'\b(kg|g)\b', nome_limpo):
        return "KG"

    # Regra 5: Nome longo → assume UNI
    if len(palavras) >= 4:
        return "UNI"

    # Default: considera vendido a peso
    return "KG"

def processar_csv(caminho_entrada, caminho_saida):
    df = pd.read_csv(caminho_entrada, sep=';')

    unit_types = []
    weights = []
    lengths = []
    widths = []
    heights = []

    for nome in df['Nome']:
        unit_type = inferir_unit_type(nome)
        medidas = parse_medidas(nome)

        unit_types.append(unit_type)
        weights.append(medidas['weight'])
        lengths.append(medidas['length'])
        widths.append(medidas['width'])
        heights.append(medidas['height'])

    df['unit_type'] = unit_types
    df['weight'] = weights
    df['length'] = lengths
    df['width'] = widths
    df['height'] = heights
    df['name'] = df['Nome'].apply(lambda x: str(x))
    df['barcodes'] = df['Código de barras'].apply(float_to_string)
    df['barcodes'] = df['barcodes'].apply(lambda x: format_to_ean(x[0]) if x else [])
    df['promo_price'] = df['Promocao'].apply(promo_price_formater)
    df['price'] = df['Preço regular'].apply(price_formater)
    df['visible'] = df['ativo']
    df['internal_code'] = df['Código interno'].apply(internal_code_formater)
    df['stock'] = df['estoque'].apply(stock_formater)

    df.drop(columns=['Nome', 'Código de barras', 'Promocao', 'Preço regular', 'ativo', 'Código interno', 'estoque'], inplace=True)
    df = df[['internal_code', 'name', 'unit_type', 'price', 'visible', 'stock', 'barcodes', 'promo_price', 'Data termino promocao', 'weight', 'length', 'width', 'height']]

    df.to_csv(caminho_saida, sep=';', index=False)
    print(f"Arquivo processado salvo em: {caminho_saida}") 
    

processar_csv("items.csv", "produtos_processados.csv")
# df = pd.read_csv('items.csv', sep=';')
# df_treated = pd.DataFrame()

# df_treated.to_csv('cols_validation.csv', index=False)


