import pandas as pd
import re
from unidecode import unidecode

def parse_medidas(nome):
    nome = unidecode(str(nome).lower())
    medidas = {'length': None, 'width': None, 'height': None, 'weight': None}

    # Peso
    peso_match = re.search(r'(\d+(?:[\.,]\d+)?)\s*(kg|g|l|ml)', nome)
    if peso_match:
        valor, unidade = peso_match.groups()
        valor = float(valor.replace(',', '.'))
        if unidade == 'g':
            valor /= 1000
        elif unidade == 'ml':
            valor /= 1000
        medidas['weight'] = round(valor, 3)

    # Medidas (comprimento x largura x altura) - aceita "x", "/", "X"
    medidas_match = re.search(r'(\d+(?:[\.,]\d+)?)\s*[xX/]\s*(\d+(?:[\.,]\d+)?)(?:\s*[xX/]\s*(\d+(?:[\.,]\d+)?))?', nome)
    if medidas_match:
        l, w, h = medidas_match.groups()
        medidas['length'] = float(l.replace(',', '.'))
        medidas['width'] = float(w.replace(',', '.'))
        if h:
            medidas['height'] = float(h.replace(',', '.'))

    return medidas

def inferir_unit_type(nome):
    nome = unidecode(str(nome).lower())
    palavras = nome.split()

    # Se contém palavras que indicam unidade explícita
    if re.search(r'\b(un|unid|unidade|pct|pacote|cx|caixa|frasco|galao)\b', nome):
        return "UNI"

    # Se contém números com medidas → vendido por unidade
    if re.search(r'\d+(kg|g|l|ml|m|m2)\b', nome):
        return "UNI"

    # Se contém apenas a sigla de medidas sem número → provavelmente peso
    if re.search(r'\b(kg|g|l|ml|m|m2)\b', nome):
        return "KG"

    # Se nome tem 4 ou mais palavras → item mais elaborado, assume unidade
    if len(palavras) >= 3:
        return "UNI"

    # Default: item simples, assume KG
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

    df.to_csv(caminho_saida, sep=';', index=False)
    print(f"Arquivo processado salvo em: {caminho_saida}")

# Exemplo de uso
processar_csv("items.csv", "produtos_processados.csv")
