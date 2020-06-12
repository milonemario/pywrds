"""
Provides processing functions for CRSP data
"""


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
