import os
import pandas as pd
import numpy as np
import json
import logging

from .utils import (
    infer_unit_type, measure_parser, float_to_string, format_to_ean,
    promo_price_formater, price_formater, internal_code_formater,
    stock_formater, process_promo_dates
)

logging.basicConfig(filename='processing.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class ProductDataProcessor:
    def __init__(self, input_path: str):
        self.input_path = input_path
        self.df = None
        self.batches = []

    def load_and_process(self):
        logging.info("Starting load_and_process")
        df = pd.read_csv(self.input_path, sep=';')

        unit_types, weights, lengths, widths, heights = [], [], [], [], []

        for name in df['Nome']:
            unit_types.append(infer_unit_type(name))
            measures = measure_parser(name)
            weights.append(measures['weight'])
            lengths.append(measures['length'])
            widths.append(measures['width'])
            heights.append(measures['height'])

        df['unit_type'] = unit_types
        df['weight'] = weights
        df['length'] = lengths
        df['width'] = widths
        df['height'] = heights

        df['name'] = df['Nome'].astype(str)
        df['barcodes'] = df['Código de barras'].apply(float_to_string)
        df['barcodes'] = df['barcodes'].apply(lambda x: format_to_ean(x[0]) if x else [])
        df['promo_price'] = df['Promocao'].apply(promo_price_formater)
        df['price'] = df['Preço regular'].apply(price_formater)
        df['visible'] = df['ativo']
        df['internal_code'] = df['Código interno'].apply(internal_code_formater)
        df['stock'] = df['estoque'].apply(stock_formater)
        df[['promo_start_at', 'promo_end_at']] = df['Data termino promocao'].apply(process_promo_dates).apply(pd.Series)

        drop_columns = ['Nome', 'Código de barras', 'Promocao', 'Preço regular', 'ativo', 'Código interno', 'estoque', 'Data termino promocao']
        df.drop(columns=drop_columns, inplace=True)

        df = df[['internal_code', 'name', 'unit_type', 'price', 'visible', 'stock', 'barcodes', 'promo_price', 'weight', 'length', 'width', 'height', 'promo_end_at', 'promo_start_at']]
        df.replace({np.nan: None}, inplace=True)

        self.df = df
        logging.info("Finished load_and_process")

    def create_batches(self):
        logging.info("Starting create_batches")
        os.makedirs('batches', exist_ok=True)
        self.batches = []

        for i in range(0, len(self.df), 1000):
            batch = {"products": self.df[i:i+1000].apply(lambda row: row.to_dict(), axis=1).tolist()}
            self.batches.append(batch)

            with open(f'batches/batch_{i//1000 + 1}.json', 'w', encoding='utf-8') as json_file:
                json.dump(batch, json_file, ensure_ascii=False, indent=4)

        logging.info("Finished create_batches")
        return self.batches
