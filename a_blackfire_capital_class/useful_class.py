import itertools
import time
import multiprocessing
from typing import Callable, Tuple, Union
import pandas as pd
import numpy as np
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
    def _ds_using_builtin_set_type(edges: list, vertices: list) -> set:
        """
        Description:
        ------------

        This function is used to find all the independents  subsets between two sets. It's returns
        the group of subsets with their constituents.

        Parameter:
        ----------

        :param edges: list of Tuple of relation between the elements in sets. Ex: [('111F', 'M27')]
        :param vertices: list of all elements in the sets. Ex: ['111F', 'M27']

        Return:
        -------

        :return: sets of uniques subsets.
        """
        # given an element, create a set with only this element as member
        def make_set(v):
            return frozenset([v])

        # create big set of sets
        sets = set([make_set(v) for v in vertices])

        # find a set containing element x in a list of all sets
        def find_set(x):
            for subset in sets:
                if x in subset:
                    return subset

        # create a combined set containing all elements of both sets, destroy original sets
        def union(set_u, set_v):
            sets.add(frozenset.union(set_u, set_v))
            sets.remove(set_u)
            sets.remove(set_v)

        # main algorithm: find all connected components
        for (u, v) in edges:
            set_u = find_set(u)
            set_v = find_set(v)

            if set_u != set_v:
                union(set_u, set_v)

        return sets

    def convert_wiod_code_to_bea_naics(self) -> pd.DataFrame:
        """
        Description:
        ------------

        This function is used to map NACE code and NAICS code.

        Return:
        ------

        :return:  DataFrame of mapping between NACE code and BEA code. The columns group is add to
        group the multiple relations between NACE and BEA into 1 to 1 set.
        """

        def get_translations_wiod_code_to_bea_naics(naics):
            return normalize('NFKD', naics).replace(' ', '')

        # Open excel files containing the informations.
        my_path = Path(__file__).parent.parent.resolve()
        my_path = str(my_path) + '/e_blackfire_capital_files/'
        df = pd.read_excel(str(my_path) + 'wiod_code_to_bea_naics.xlsx', sheet_name='convert', index_col=None)
        df = df[['NAICS code', 'NACE code', 'Industries in WIOT SUTs']]
        df = df.fillna(method='ffill')
        df.drop_duplicates(subset=['NAICS code', 'NACE code', 'Industries in WIOT SUTs'], inplace=True)
        df = df.astype(str)
        df.loc[:, 'NAICS code'] = df.apply(lambda x: get_translations_wiod_code_to_bea_naics(x['NAICS code']), axis=1)

        # Transform the mapping into 1 to 1 set.
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
    def map_bea_and_naics() -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to map BEA Code with the NAICS.

        Return:
        -------

        :return: DataFrame of mapping between NAICS and BEA code.
        """

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

    def get_custom_group_for_io(self) -> pd.DataFrame:

        """
        Description:
        ------------

        This function returns a dataFrame with a mapping between NACE code, BEA code and NAICS.
        Thus the columns group give a custom clustering to group the informations for later work
        in the IO tables.

        Return:
        -------

        :return: DataFrame of mapping NACE code, BEA code and NAICS.
        """

        my_path = Path(__file__).parent.parent.resolve()
        my_path = str(my_path) + '/e_blackfire_capital_files/'
        custom_group = pd.read_excel(my_path + 'nace_to_bea_mapping.xlsx', sheet_name='mapping', index_col=None)
        bea_and_naics = self.map_bea_and_naics()

        custom_group = pd.merge(custom_group, bea_and_naics, on=['bea_sector'], how='left')

        return custom_group

    def get_wiod_table(self) -> pd.DataFrame:
        """
        Description:
        ------------

        This function is used to import the IO tables.

        Return:
        -------
        :return: DataFrame of IO tables.
        """

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
        data = data.groupby(['Year', 'Origin', 'Code']).sum().reset_index()

        # nace_group.reset_index(inplace=True)
        # print(frozenset(data.columns) - frozenset(nace_group['group']))
        # print(frozenset(nace_group['group']) - frozenset(data.columns))

        # Calculate finals demands and Gov consumption.
        data.loc[:, 'FD'] = data[['CONS_h', 'CONS_np', 'CONS_g']].sum(axis=1)
        # data.loc[:, 'GOV'] = data[['Q', 'R_S', 'N', 'U']].sum(axis=1)

        return data

    def get_leontief_matrix(self, year: int) -> pd.DataFrame:

        """
        Description:
        ------------
        This function is used to compute the leontief matrix for a given year. Leontief matrix is given by:
        F = [I - (I - M) * A] * X. Where F is final demands, M is imports matrix and A is domestic matrix.

        Parameter:
        ----------
        :param year:
        :return:
        """
        year = year - 5 if year > 2004 else 2000

        header = ['Origin', 'Code', 'A01', 'A02', 'B', 'C10-C12', 'C13-C15', 'C16', 'C17', 'C18', 'C19',
                  'C21', 'C22', 'C23', 'C24', 'C25', 'C26', 'C27', 'C28', 'C29', 'C30', 'C31_C32', 'E36', 'E37-E39',
                  'F', 'G46', 'G47', 'H49', 'H50', 'H51', 'H52', 'I', 'J58', 'J61', 'J62_J63', 'K64', 'K65', 'K66',
                  'L68', 'M74_M75', 'N', 'O84', 'P85', 'Q', 'R_S', 'S96']

        # get the IO tables
        niot_matrix = self.get_wiod_table()
        mask = (niot_matrix['Origin'].isin(['Domestic', 'Imports', 'TOT'])) & (niot_matrix['Year'].astype(int) == year)
        niot_matrix = niot_matrix[mask][header]

        new_header = header[:]
        new_header.remove('Code')
        new_header.remove('Origin')

        # Divide each column by the total output.
        total = niot_matrix[niot_matrix['Code'] == 'GO'][new_header].values[0]
        niot_matrix.loc[:, new_header] = niot_matrix.loc[:, new_header]/total

        # Apply the formula of the leontief matrix. A = [I - (I - M) * D]
        niot_matrix = niot_matrix[niot_matrix['Code'].isin(new_header)].reset_index(drop=True)
        niot_matrix.set_index('Code', inplace=True)
        dom_matrix = niot_matrix[(niot_matrix['Origin'] == 'Domestic')][new_header]
        imp_matrix = niot_matrix[(niot_matrix['Origin'] == 'Imports')][new_header]
        identity_matrix = np.identity(len(new_header))

        leontief_matrix = (identity_matrix - dom_matrix)
        # print(np.fill_diagonal(leontief_matrix.values, 0))
        # print(leontief_matrix)
        # leontief_matrix.to_excel('t.xlsx')
        return leontief_matrix

    @staticmethod
    def apply_ranking(group: pd.DataFrame, by: str, percentile: list) -> pd.DataFrame:

        """"
        Description:
        ------------

        This function take a DataFrame as input and return a columns with a ranking from percentile range
        given the feature.

        :param
        group: DataFrame containing the values to rank
        by:  Name of the column to rank

        :return
        DataFrame containing one column ranking with the features ranks.

        """""

        labels = [str(i + 1) for i in range(len(percentile) - 1)]
        tab_group = group[[by]].quantile(np.array(percentile), numeric_only=False)
        group = group.fillna(np.nan)

        tab_group['labels'] = ['0'] + labels
        x = tab_group[[by, 'labels']].drop_duplicates([by])
        labels = list(x['labels'])
        labels.remove('0')
        group['ranking_' + by] = pd.cut(group[by], x[by], labels=labels). \
            values.add_categories('0').fillna('0')

        return group
MiscellaneousFunctions().get_leontief_matrix(2010)