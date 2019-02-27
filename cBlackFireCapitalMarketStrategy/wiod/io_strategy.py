import pandas as pd
# import unidecode
# word = unidecode.unidecode(word)
from unicodedata import normalize


def convert_wiod_code_to_bea_naics():
    df = pd.read_excel('wiod_code_to_bea_naics.xlsx', sheet_name='convert', index_col=None)
    df = df[['NAICS code', 'NACE code', 'Industries in WIOT SUTs']]
    df = df.fillna(method='ffill')
    df = df.astype(str)
    d = dict()

    def get_translations_wiod_code_to_bea_naics(naics, nace):
        d[nace] = normalize('NFKD', naics).replace(' ', '')

    df.apply(lambda x: get_translations_wiod_code_to_bea_naics(x['NAICS code'], x['NACE code']), axis=1)

    return d


def convert_naics_to_bea_naics():
    df = pd.read_excel('wiod_code_to_bea_naics.xlsx', sheet_name='naics', index_col=None)

    df = df.fillna(method='ffill')
    df = df.astype(str)
    d = dict()

    def get_translations_wiod_code_to_bea_naics(nace, naics):
        d[naics[0:3]] = normalize('NFKD', nace)

    df.apply(lambda x: get_translations_wiod_code_to_bea_naics(x['Summary'], x['Detail']), axis=1)
    return d


def import_wiod_table(wiod_to_naics):
    data = pd.read_excel('niot usa.xlsx', sheet_name='National IO-tables', index_col=None)
    data.loc[:, 'Code'] = data['Code'].replace(wiod_to_naics)
    data.rename(columns=wiod_to_naics, inplace=True)
    data = data[(data['Year'].isna() == False)].reset_index(drop=True)
    print(data.head(10))
    print(data.columns)
    # group columns by bea naics and apply the sum
    data = data.groupby(data.columns, axis=1).sum()
    print(data.groupby(['Year', 'Code', 'Origin']).sum().reset_index())
    print(data.head(10))


wiod_to_naics = convert_wiod_code_to_bea_naics()
import_wiod_table(wiod_to_naics)
# convert_naics_to_bea_naics()
