import itertools
import time
import multiprocessing
from typing import Callable, Tuple, Union
import pandas as pd
import sys
import smtplib, ssl


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
