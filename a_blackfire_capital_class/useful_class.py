import itertools
import time
import multiprocessing
from typing import Callable, Tuple, Union
import pandas as pd
import sys
import smtplib, ssl
from pathlib import Path
from unicodedata import normalize


class CustomMultiprocessing:

    def __init__(self, **kwargs):

        self._num_cpu = kwargs.get('num_cpu', multiprocessing.cpu_count() - 1)

    def exec_in_parallel(self, tab_parameter: list,
                         func: Callable[[Tuple[str, pd.DataFrame]], Union[pd.DataFrame, pd.Series]],
                         logger: Callable[[str], None] = sys.stdout) -> pd.DataFrame:
        """
        Description:
        ------------

        Performs a Task in parallel.

        Parameter:
        ----------

        :param tab_parameter: list of the input arguments for the function
        :param func: function to call in parallel
        :param logger: function to write in the log.

        :type tab_parameter: list
        :type func: Callable

        Return:
        ------

        :return DataFrame of the Task to perform
        :rtype pd.DataFrame

        Usage:
        -----

        tab_parameter = [(my_date,) for my_date in date_tab]
        summary = CustomMultiprocessing().exec_in_parallel(tab_parameter, get_monthly_stocks_price_from_mongodb)
        summary.head(15)

        USD_to_curr  adj_factor          csho   ...   rcvar nrec  nrcvar
        0         1.000     1.00000  1.238157e+09   ...    None  NaN     NaN
        1         1.000     1.00000  5.415934e+08   ...    None  NaN     NaN
        2         1.000     1.00000  2.457172e+09   ...    None  NaN     NaN
        3         1.000     1.00000  1.094957e+09   ...    None  NaN     NaN
        4         1.000     1.51037  9.093184e+08   ...    None  NaN     NaN
        5         1.000     1.00000  8.617718e+08   ...    None  NaN     NaN
        6         1.000     1.00000  8.000000e+07   ...    None  NaN     NaN
        7         1.000     1.00000  1.220295e+08   ...    None  NaN     NaN
        8        15.871     1.00000  8.506744e+06   ...    None  NaN     NaN
        9         1.000     1.00000  6.719499e+08   ...    None  NaN     NaN


        """

        start = time.time()
        logger.flush()
        logger.write("\nUsing {} CPUs in parallel...\n".format(self._num_cpu))

        with multiprocessing.Pool(self._num_cpu) as pool:
            result = pool.starmap_async(func, tab_parameter)
            cycler = itertools.cycle('\|/―')
            while not result.ready():
                value = "\rTasks left: {} / {}. {}\t".format(result._number_left, len(tab_parameter),
                                                                  next(cycler))
                logger.write(value)
                logger.flush()
                time.sleep(0.1)
            got = result.get()
        logger.write("\nTasks completed. Processed {} group in {:.1f}s\n".format(len(got), time.time() - start))

        return pd.concat(got)


class SendSimulationState:

    def __init__(self, message, **kwargs):

        self._message = message
        self._smtp_server = "smtp.gmail.com"
        self._port = 587
        self._sender_email = "blackfirecapitaldev@gmail.com"
        self._password = "blackfirecapitaldev09"
        self._receiver_email = kwargs.get('to', 'noupougi@gmail.com')

    def send_email(self):

        # Create a secure SSL context
        context = ssl.create_default_context()

        # Try to log in to server and send email
        try:
            server = smtplib.SMTP(self._smtp_server,self._port)
            server.ehlo() # Can be omitted
            server.starttls(context=context) # Secure the connection
            server.ehlo() # Can be omitted
            server.login(self._sender_email, self._password)
            message = "Subject: Simulation Update. \n" + self._message

            server.sendmail(self._sender_email, self._receiver_email, message)
        except Exception as e:
            # Print any error messages to stdout
            print(e)
        finally:
            server.quit()


class MiscellaneousFunctions:

    @staticmethod
    def _ds_using_builtin_set_type(edges, vertices):

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

    def convert_wiod_code_to_bea_naics(self):

        def get_translations_wiod_code_to_bea_naics(naics):
            return normalize('NFKD', naics).replace(' ', '')

        my_path = Path(__file__).parent.parent.resolve()
        my_path = str(my_path) + '/e_blackfire_capital_files/'
        df = pd.read_excel(str(my_path) + 'wiod_code_to_bea_naics.xlsx', sheet_name='convert', index_col=None)
        df = df[['NAICS code', 'NACE code', 'Industries in WIOT SUTs']]
        df = df.fillna(method='ffill')
        df.drop_duplicates(subset=['NAICS code', 'NACE code', 'Industries in WIOT SUTs'], inplace=True)
        df = df.astype(str)
        df.loc[:, 'NAICS code'] = df.apply(lambda x: get_translations_wiod_code_to_bea_naics(x['NAICS code']), axis=1)

        vertices = df['NAICS code'].unique().tolist() + df['NACE code'].unique().tolist()
        edges = [(naics, nace) for naics, nace in df[['NAICS code', 'NACE code']].values.tolist()]
        sets = self._ds_using_builtin_set_type(edges, vertices)
        df.loc[:, 'group'] = None
        for s in sets:
            for value in s:
                mask = (df.loc[:, 'NAICS code'] == value) | (df.loc[:, 'NACE code'] == value)
                df.loc[mask, 'group'] = s

        df.rename(columns={'NAICS code': 'bea_sector', 'NACE code': 'nace_sector'}, inplace=True)

        return df

    @staticmethod
    def map_bea_and_naics():

        my_path = Path(__file__).parent.parent.resolve()
        my_path = str(my_path) + '/e_blackfire_capital_files/'
        df = pd.read_excel(str(my_path) + 'wiod_code_to_bea_naics.xlsx', sheet_name='naics', index_col=None)
        df = df[['Summary', 'Detail']]
        df = df.fillna(method='ffill')
        df = df.astype(str)
        df.loc[:, 'sector'] = df['Detail'].str[:3]
        df.drop_duplicates(subset=['Summary', 'sector'], inplace=True)
        df.rename(columns={'Summary': 'bea_sector'}, inplace=True)

        return df

    def get_custom_group_for_io(self):

        my_path = Path(__file__).parent.parent.resolve()
        my_path = str(my_path) + '/e_blackfire_capital_files/'
        custom_group = pd.read_excel(my_path + 'nace_to_bea_mapping.xlsx', sheet_name='mapping', index_col=None)

        bea_and_naics = self.map_bea_and_naics()
        custom_group = pd.merge(custom_group.drop_duplicates(subset=['bea_sector']),
                                bea_and_naics.drop_duplicates(subset=['sector']),
                                on=['bea_sector'], how='left')
        return custom_group

    def get_wiod_table(self):


        my_path = Path(__file__).parent.parent.resolve()
        my_path = str(my_path) + '/e_blackfire_capital_files/'
        data = pd.read_excel(my_path + 'niot usa.xlsx', sheet_name='National IO-tables', index_col=None)

        nace_group = self.get_custom_group_for_io()
        nace_group = nace_group.drop_duplicates(['nace_sector'])[['nace_sector', 'group']].set_index('nace_sector')
        wiod_to_naics = nace_group.to_dict()

        # Replace WIOT codes by BEA NAICS
        data.loc[:, 'Code'] = data['Code'].replace(wiod_to_naics['group'])
        data.rename(columns=wiod_to_naics['group'], inplace=True)
        data = data[(data['Year'].isna() == False)].reset_index(drop=True)

        # group columns by bea naics and apply the sum
        data = data.groupby(data.columns, axis=1).sum()
        data = data.groupby(['Year','Origin', 'Code']).sum().reset_index()

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
g = MiscellaneousFunctions().import_wiod_table()

# g.drop_duplicates(['nace_sector']).to_excel('test_.xlsx')