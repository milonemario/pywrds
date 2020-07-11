"""
Provides processing functions for the CBOE data
"""

from pywrds import wrds_module


class cboe(wrds_module):

    def __init__(self, w):
        wrds_module.__init__(self, w)
        # Initialize values
        self.col_date = 'date'

    def get_fields(self, data, fields):
        key = ['date']
        df = self.w.open_data(self.cboe, key+fields)
        # Merge to the use data
        dfu = self.w.open_data(data, [self.w.col_date])
        dfu['date'] = dfu[self.w.col_date]
        dfin = dfu.merge(df, how='left', on=key)
        dfin.index = dfu.index
        return(dfin[fields])
