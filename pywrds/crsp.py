"""
Provides processing functions for CRSP data
"""

import pandas as pd
import numpy as np


def daily_returns(w, file_data, file_dsf, col_date, ndays=1):
    r"""
    Add the compounded daily returns over 'ndays' days to the data 'file_data'
    using returns contained in 'file_dsf'. It computes the compounded returns
    from the date specified in column 'column' from 'file_data'
    """
    # Open the necessary data
    cols_data = ['permno', col_date]
    cols_dsf = ['permno', 'date', 'ret']
    data = w.open_data(file_data, cols_data)
    dsf = w.open_data(file_dsf, cols_dsf)
    # Remove the unnecessar permno from dsf
    pn_list = pd.unique(data.permno)
    dsf = dsf[dsf.permno.isin(pn_list)]
    # Create the timeseirs index
    dsf = dsf.set_index('date')
    # Compute the compounded returns
    if ndays > 1:
        dsf['ln1ret'] = np.log(1 + dsf.ret)
        s = dsf.groupby('permno').rolling(ndays).ln1ret.sum()
        dsf['s'] = s.shift(1-ndays).values
        dsf['cret'] = np.exp(dsf.s) - 1
        dsf = dsf.drop(['ln1ret', 's'], 1)
    else:
        dsf['cret'] = dsf.ret
    dsf = dsf.drop('ret', 1).reset_index()
    # Merge the cumulative return to the data
    m = data.merge(dsf, how='left', left_on=cols_data,
                   right_on=['permno', 'date'])
    return(m.cret)


def get_yearly_return(w, sf, data_frequency='monthly'):
    # Compute compounded yearly returns on monthly or daily stock data
    None


def delist_dummy(w):
    # Create a delist dummy
    None


def add_ret2anndates(w, file_original, file_crsp_daily, anndates, x_days):

    # Need columns GVKEY, datetime, anndates

    # Check if everything is alright(e.g. no return data in original dataset),
    # otherwise raise exception

    # Add return data to original dataset on "permno"

    # Create empty dataframe with x days + 1 columns
    # df_ret = pd.DataFrame(columns=range(x_days+1))
    df = pd.merge(file_original, file_crsp_daily["ret"],
                  how='left', on='permno')
    return df
