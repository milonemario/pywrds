"""
Provides processing functions for CRSP data
"""

import pandas as pd


def add_daily_return(w, file_data, file_dsf, column, ndays=1):
    r"""
    Add the compounded daily returns over 'ndays' days to the data 'file_data'
    using returns contained in 'file_dsf'. It computes the coumpounded returns
    from the date specified in column 'column' from 'file_data'
    """


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
