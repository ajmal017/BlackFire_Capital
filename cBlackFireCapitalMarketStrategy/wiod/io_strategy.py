import pandas as pd
import numpy as np
from pathlib import Path
from unicodedata import normalize


def ds_using_builtin_set_type(edges, vertices):

    # given an element, create a set with only this element as member
    def MAKE_SET(v):
        return frozenset([v])

    # create big set of sets
    sets = set([MAKE_SET(v) for v in vertices])

    # find a set containing element x in a list of all sets
    def FIND_SET(x):
        for subset in sets:
            if x in subset:
                return subset

    # create a combined set containing all elements of both sets, destroy original sets
    def UNION(set_u, set_v):
        sets.add(frozenset.union(set_u, set_v))
        sets.remove(set_u)
        sets.remove(set_v)

    # main algorithm: find all connected components
    for (u, v) in edges:
        set_u = FIND_SET(u)
        set_v = FIND_SET(v)

        if set_u != set_v:
            UNION(set_u, set_v)

    return sets


def convert_wiod_code_to_bea_naics():

    def get_translations_wiod_code_to_bea_naics(naics):
        return normalize('NFKD', naics).replace(' ', '')

    my_path = Path(__file__).parent.parent.parent.resolve()
    my_path = str(my_path) + '/e_blackfire_capital_files/'
    df = pd.read_excel(str(my_path) + 'wiod_code_to_bea_naics.xlsx', sheet_name='convert', index_col=None)
    df = df[['NAICS code', 'NACE code', 'Industries in WIOT SUTs']]
    df = df.fillna(method='ffill')
    df.drop_duplicates(subset=['NAICS code', 'NACE code', 'Industries in WIOT SUTs'], inplace=True)
    df = df.astype(str)
    df.loc[:, 'NAICS code'] = df.apply(lambda x: get_translations_wiod_code_to_bea_naics(x['NAICS code']), axis=1)

    vertices = df['NAICS code'].unique().tolist() + df['NACE code'].unique().tolist()
    edges = [(naics, nace) for naics, nace in df[['NAICS code', 'NACE code']].values.tolist()]
    sets = ds_using_builtin_set_type(edges, vertices)
    df.loc[:, 'group'] = None
    for s in sets:
        for value in s:
            mask = (df.loc[:, 'NAICS code'] == value) | (df.loc[:, 'NACE code'] == value)
            df.loc[mask, 'group'] = s

    return df


def mapping_bea_and_naics():

    my_path = Path(__file__).parent.parent.parent.resolve()
    my_path = str(my_path) + '/e_blackfire_capital_files/'
    df = pd.read_excel(str(my_path) + 'wiod_code_to_bea_naics.xlsx', sheet_name='naics', index_col=None)
    df = df[['Summary', 'Detail']]
    df = df.fillna(method='ffill')
    df = df.astype(str)
    df.loc[:, 'sector'] = df['Detail'].str[:3]
    df.drop_duplicates(subset=['Summary', 'sector'], inplace=True)
    df.rename(columns={'Summary': 'bea_sector'}, inplace=True)

    return df


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
    wiod_to_naics = mapping_bea_and_naics()
    # niot_matrix = import_wiod_table(wiod_to_naics)
    # return_leontief_matrix(niot_matrix)
    # convert_naics_to_bea_naics()
