from pathlib import Path
import pandas as pd
import numpy as np
# import unidecode
# word = unidecode.unidecode(word)
from unicodedata import normalize


def convert_wiod_code_to_bea_naics():
    my_path = Path(__file__).parent.parent.parent.resolve()
    my_path = str(my_path) + '/eBlackFireCapitalFiles/'
    df = pd.read_excel(str(my_path) + 'wiod_code_to_bea_naics.xlsx', sheet_name='convert', index_col=None)
    df = df[['NAICS code', 'NACE code', 'Industries in WIOT SUTs']]
    df = df.fillna(method='ffill')
    df = df.astype(str)
    d = dict()

    def get_translations_wiod_code_to_bea_naics(naics, nace):
        d[nace] = normalize('NFKD', naics).replace(' ', '')

    df.apply(lambda x: get_translations_wiod_code_to_bea_naics(x['NAICS code'], x['NACE code']), axis=1)

    return d


def convert_naics_to_bea_naics():

    my_path = Path(__file__).parent.parent.parent.resolve()
    my_path = my_path + '/eBlackFireCapitalFiles/'
    df = pd.read_excel(my_path + 'wiod_code_to_bea_naics.xlsx', sheet_name='naics', index_col=None)

    df = df.fillna(method='ffill')
    df = df.astype(str)
    d = dict()

    def get_translations_wiod_code_to_bea_naics(nace, naics):
        d[naics[0:3]] = normalize('NFKD', nace)

    df.apply(lambda x: get_translations_wiod_code_to_bea_naics(x['Summary'], x['Detail']), axis=1)
    return d


def import_wiod_table(wiod_to_naics):

    my_path = Path(__file__).parent.parent.parent.resolve()
    my_path = str(my_path) + '/eBlackFireCapitalFiles/'

    # Replace WIOT codes by BEA NAICS
    data = pd.read_excel(my_path + 'niot usa.xlsx', sheet_name='National IO-tables', index_col=None)
    data.loc[:, 'Code'] = data['Code'].replace(wiod_to_naics)
    data.rename(columns=wiod_to_naics, inplace=True)
    data = data[(data['Year'].isna() == False)].reset_index(drop=True)

    # group columns by bea naics and apply the sum
    data = data.groupby(data.columns, axis=1).sum()
    data = data.groupby(['Year', 'Code', 'Origin']).sum().reset_index()

    # Calculate finals demands
    data.loc[:, 'FD'] = data[['CONS_h', 'CONS_np', 'CONS_g', 'GFCF', 'INVEN', 'EXP']].sum(axis=1)
    return data


def return_leontief_matrix(niot_matrix):

    header = ['Year', 'Code', 'Origin', '111CA', '113FF', '213', '22', '23', '311FT', '315AL', '321', '322', '323',
              '324', '325', '326', '327', '331', '332', '333', '334', '335', '3361MV', '3364OT', '339', '42', '481',
              '483', '486', '487OS', '493', '4A0', '511', '513', '521CI', '523', '525', '5412OP', '5415', '55', '61',
              '722', '81', 'D35', 'GSLE', 'N', 'ORE', 'Q', 'R_S', 'Used']

    def get_leontief_matrix(group, header):

        new_header = header[:]
        new_header.remove('Year')
        new_header.remove('Code')
        new_header.remove('Origin')

        total = group[group['Code'] == 'GO'][new_header].values[0]
        group = np.identity(len(new_header)) - group[group['Code'].isin(new_header)][new_header]/total
        group = np.linalg.inv(group)

        return pd.DataFrame(group, new_header, new_header)

    niot_matrix = niot_matrix[niot_matrix['Origin'].isin(['Domestic', 'TOT'])][header]
    niot_matrix.groupby(['Year']).apply(get_leontief_matrix, header).to_excel('leontief.xlsx')



if __name__ == "__main__":

    # my_path = Path(__file__).parent.parent.parent.resolve()
    # print(my_path)
    wiod_to_naics = convert_wiod_code_to_bea_naics()
    niot_matrix = import_wiod_table(wiod_to_naics)
    return_leontief_matrix(niot_matrix)
    # convert_naics_to_bea_naics()
