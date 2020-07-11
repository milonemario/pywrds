"""
Provides processing functions for CRSP data
"""

from pywrds import wrds_module
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal


class crsp(wrds_module):

    def __init__(self, w):
        wrds_module.__init__(self, w)
        # Initialize values
        self.col_id = 'permno'
        self.col_date = 'date'
        # For link with COMPUSTAT
        self.linktype = ['LC', 'LU']
        self.linkprim = ['P', 'C']
        # Default data frequency
        self.freq = 'M'

    def set_frequency(self, frequency):
        if frequency in ['Monthly', 'monthly', 'M', 'm']:
            self.freq = 'M'
            self.sf = self.msf
        elif frequency in ['Daily', 'daily', 'D', 'd']:
            self.freq = 'D'
            self.sf = self.dsf
        else:
            raise Exception('CRSP data frequency should by either',
                            'Monthly or Daily')

    def permno_from_gvkey(self, data):
        """ Returns CRSP permno from COMPUSTAT gvkey.
        Arguments:
            data -- User provided data.
                    Required columns: [gvkey, 'col_date']
            link_table --   WRDS provided linktable (ccmxpf_lnkhist)
            linktype -- Default: [LC, LU]
            linkprim -- Default: [P, C]
        """
        # Columns required from data
        key = ['gvkey', self.w.col_date]
        # Columns required from the link table
        cols_link = ['gvkey', 'lpermno', 'linktype', 'linkprim',
                     'linkdt', 'linkenddt']
        # Open the data
        data_key = self.w.open_data(data, key).drop_duplicates().dropna()
        link = self.w.open_data(self.linktable, cols_link)
        link = link.dropna(subset=['gvkey', 'lpermno', 'linktype', 'linkprim'])
        # Retrieve the specified links
        link = link[(link.linktype.isin(self.linktype)) &
                    (link.linkprim.isin(self.linkprim))]
        link = link[['gvkey', 'lpermno', 'linkdt', 'linkenddt']]
        # Merge the data
        dm = data_key.merge(link, how='left', on='gvkey')
        # Filter the dates (keep correct matches)
        cond1 = (pd.notna(dm.linkenddt) &
                 (dm[self.w.col_date] >= dm.linkdt) &
                 (dm[self.w.col_date] <= dm.linkenddt))
        cond2 = (pd.isna(dm.linkenddt) & (dm['datadate'] >= dm.linkdt))
        dm = dm[cond1 | cond2]
        # Rename lpermno to permno and remove unnecessary columns
        dm = dm.rename(columns={'lpermno': 'permno'})
        dm = dm[['gvkey', self.w.col_date, 'permno']]
        # Check for duplicates on the key
        n_dup = dm.shape[0] - dm[key].drop_duplicates().shape[0]
        if n_dup > 0:
            print("Warning: The merged permno",
                  "contains {:} duplicates".format(n_dup))
        # Add the permno to the user's data
        dfu = self.w.open_data(data, key)
        dfin = dfu.merge(dm, how='left', on=key)
        dfin.index = dfu.index
        return(dfin.permno.astype('float32'))

    def permno_from_cusip(self, data):
        """ Returns CRSP permno from CUSIP.
        Arguments:
            data -- User provided data.
                    Required columns: ['cusip']
                    The cusip needs to be the CRSP ncusip.
        """
        dfu = self.w.open_data(data, ['cusip'])
        cs = dfu.drop_duplicates()
        pc = self.w.open_data(self.msenames, ['ncusip', 'permno'])
        pc = pc.drop_duplicates()
        cs = cs.merge(pc, how='left', left_on=['cusip'], right_on=['ncusip'])
        csf = cs[['cusip', 'permno']].dropna().drop_duplicates()
        dfin = dfu.merge(csf, how='left', on='cusip')
        dfin.index = dfu.index
        return(dfin.permno.astype('float32'))

    def _adjust_shares(self, data, col_shares):
        """ Adjust the number of shares using CRSP cfacshr field.
        Arguments:
            data -- User provided data.
                    Required fields: [permno, 'col_shares', 'col_date']
            col_shares --   The field with the number of shares from data.
            col_date -- The date field from data to use to compute the
                        adjustment.
        """
        # Open and prepare the user data
        cols = ['permno', col_shares, self.w.col_date]
        dfu = self.w.open_data(data, cols)
        index = dfu.index
        dt = pd.to_datetime(dfu[self.w.col_date]).dt
        dfu['year'] = dt.year
        dfu['month'] = dt.month
        # Open and prepare the CRSP data
        cols = ['permno', 'date', 'cfacshr']
        df = self.w.open_data(self.msf, cols)
        dt = pd.to_datetime(df.date).dt
        df['year'] = dt.year
        df['month'] = dt.month
        # Merge the data
        key = ['permno', 'year', 'month']
        dfu = dfu[key+[col_shares]].merge(df[key+['cfacshr']],
                                          how='left', on=key)
        dfu.loc[dfu.cfacshr.isna(), 'cfacshr'] = 1
        # Compute the adjusted shares
        dfu['adj_shares'] = dfu[col_shares] * dfu.cfacshr
        dfu.index = index
        return(dfu.adj_shares.astype('float32'))

    def _get_fields_dsf(self, fields, data=None):
        """ Returns the fields from CRSP.
        This function is only used internally for the CRSP module.
        Arguments:
            fields --   Fields from file_fund
            data -- User provided data
                    Required columns: [permno, date]
                    If none given, returns the entire compustat with key.
                    Otherwise, return only the fields with the data index.
        """
        key = ['permno', 'date']
        # Get the fields
        # Note: CRSP data is clean without duplicates
        df = self.w.open_data(self.dsf, key+fields)
        # Construct the object to return
        if data is not None:
            # Merge and return the fields
            data_key = self.w.open_data(data, key)
            index = data_key.index
            dfin = data_key.merge(df, how='left', on=key)
            dfin.index = index
            return(dfin[fields])
        else:
            # Return the entire dataset with keys
            return(df)

    def get_fields_daily(self, fields, data):
        """ Returns the fields from CRSP daily.
        Arguments:
            fields --   Fields from file_fund
            data -- User provided data
                    Required columns: [permno, date]
                    If none given, returns the entire compustat with key.
                    Otherwise, return only the fields with the data index.
        Requires:
            self.w.col_date -- Date field to use for the user data
        """
        keyu = ['permno', self.w.col_date]
        dfu = self.w.open_data(data, keyu)
        dfu.loc[:, 'date'] = dfu[self.w.col_date]
        dfu[fields] = self._get_fields_dsf(fields, dfu)
        return(dfu[fields])

    def _closest_trading_date(self, dates, t='past'):
        """ Return the closest trading day either in the past (t='past')
        or in the future (t='future').
        Based on opening days ofthe NYSE. """
        dates = pd.to_datetime(dates)
        nyse = mcal.get_calendar('NYSE')
        mind = dates.min()
        maxd = dates.max()
        dates_nyse = nyse.schedule(mind, maxd).market_open.dt.date
        sm = (~dates.dt.date.isin(dates_nyse)).astype(int)
        for i in range(0, 6):
            if t == 'past':
                dates = dates - pd.to_timedelta(sm, unit='d')
            else:
                dates = dates + pd.to_timedelta(sm, unit='d')
            sm = (~dates.dt.date.isin(dates_nyse)).astype(int)
        return(dates.dt.date)

    def compounded_daily_return(self, data, ndays=1, useall=True):
        r"""
        Return the compounded daily returns over 'ndays' days.
        Arguments:
            data -- User data.
                    Required columns: [permno, 'col_date']
            col_date -- Column of the dates at which to compute the return
            ndays --    Number of days to use to compute the compounded
                        daily returns. If positive, compute the return over
                        'ndays' in the future. If negative, compute the
                        return over abs(ndays) in the past.
            useall --  If True, use the compounded return of the last
                        available trading date (if ndays<0) or the
                        compounded return of the next available trading day
                        (if ndays>0).
        """
        # Open the necessary data
        key = ['permno', 'date']
        fields = ['ret']
        dsf = self._get_fields_dsf(fields)
        # Create the time series index
        dsf = dsf.set_index('date')
        # Compute the compounded returns
        if ndays == 1:
            dsf['cret'] = dsf.ret
        else:
            window = abs(ndays)
            dsf['ln1ret'] = np.log(1 + dsf.ret)
            s = dsf.groupby('permno').rolling(window).ln1ret.sum()
            if ndays < 0:
                dsf['s'] = s.values
            else:
                dsf['s'] = s.shift(1-ndays).values
            dsf['cret'] = np.exp(dsf.s) - 1
            # dsf = dsf.drop(['ln1ret', 's'], 1)
        # dsf = dsf.drop('ret', 1).reset_index()
        dsf = dsf.reset_index()
        # Use the last or next trading day if requested
        cols_data = ['permno', self.w.col_date]
        dfu = self.w.open_data(data, cols_data)
        index = dfu.index
        # Shift a maximum of 6 days
        if useall is True:
            t = 'past'
            if ndays > 0:
                t = 'future'
            dfu['date'] = self._closest_trading_date(dfu[self.w.col_date], t=t)
        else:
            dfu['date'] = dfu[self.w.col_date]
        # Merge the cumulative return to the data
        dfin = dfu.merge(dsf[key+['cret']], how='left', on=key)
        dfin.index = index
        return(dfin.cret.astype('float32'))

    def volatility_daily_return(self, data, ndays=1, useall=True):
        r"""
        Return the daily volatility of returns over 'ndays' days.
        Arguments:
            data -- User data.
                    Required columns: [permno, 'col_date']
            col_date -- Column of the dates at which to compute the return
            ndays --    Number of days to use to compute the volatility.
                        If positive, compute the volatility over
                        'ndays' in the future. If negative, compute the
                        volatility over abs(ndays) in the past.
            useall --  If True, use the volatility of the last
                        available trading date (if ndays<0) or the
                        volatility of the next available trading day
                        (if ndays>0).
        """
        # Open the necessary data
        key = ['permno', 'date']
        fields = ['ret']
        dsf = self._get_fields_dsf(fields)
        # Create the time series index
        dsf = dsf.set_index('date')
        # Compute the volatility
        window = abs(ndays)
        vol = dsf.groupby('permno').rolling(window).ret.std()
        if ndays < 0:
            dsf['vol'] = vol.values
        else:
            dsf['vol'] = vol.shift(1-ndays).values
        dsf = dsf.reset_index()
        # Use the last or next trading day if requested
        cols_data = ['permno', self.w.col_date]
        dfu = self.w.open_data(data, cols_data)
        index = dfu.index
        # Shift a maximum of 6 days
        if useall is True:
            t = 'past'
            if ndays > 0:
                t = 'future'
            dfu['date'] = self._closest_trading_date(dfu[self.w.col_date], t=t)
        else:
            dfu['date'] = dfu[self.w.col_date]
        # Merge the cumulative return to the data
        dfin = dfu.merge(dsf[key+['vol']], how='left', on=key)
        dfin.index = index
        return(dfin.vol.astype('float32'))

    def average_daily_bas(self, data, ndays=1, useall=True, bas=None):
        r"""
        Return the daily average bid-ask spread over 'ndays' days.
        Arguments:
            data -- User data.
                    Required columns: [permno, 'col_date']
            col_date -- Column of the dates at which to compute the return
            ndays --    Number of days to use to compute the bid-ask spread.
                        If positive, compute the bid-ask spread over
                        'ndays' in the future. If negative, compute the
                        bid-ask spread over abs(ndays) in the past.
            useall --  If True, use the bid-ask spread of the last
                        available trading date (if ndays<0) or the
                        bid-ask spread of the next available trading day
                        (if ndays>0).
            bas --  Type of bid and ask to use. If None, use the fields
                    'bid' and 'ask' from CRSP. If 'lohi', use the fields
                    'bidlo' and 'askhi' from CRSP.
        """
        # Open the necessary data
        key = ['permno', 'date']
        fields = ['bid', 'ask']
        if bas is None:
            fs = ['bid', 'ask']
        elif bas == 'lohi':
            fs = ['bidlo', 'askhi']
        else:
            raise Exception("'bas' argument only accepts None or 'lohi'.")
        dsf = self._get_fields_dsf(fs)
        dsf.columns = key + fields
        # Create the time series index
        dsf = dsf.set_index('date')
        # Compute the average bid-ask spread
        dsf['spread'] = dsf.ask - dsf.bid
        window = abs(ndays)
        s = dsf.groupby('permno').rolling(window).spread.mean()
        if ndays < 0:
            dsf['average'] = s.values
        else:
            dsf['average'] = s.shift(1-ndays).values
        dsf = dsf.reset_index()
        # Use the last or next trading day if requested
        cols_data = ['permno', self.w.col_date]
        dfu = self.w.open_data(data, cols_data)
        index = dfu.index
        # Shift a maximum of 6 days
        if useall is True:
            t = 'past'
            if ndays > 0:
                t = 'future'
            dfu['date'] = self._closest_trading_date(dfu[self.w.col_date], t=t)
        else:
            dfu['date'] = dfu[self.w.col_date]
        # Merge the cumulative return to the data
        dfin = dfu.merge(dsf[key+['average']], how='left', on=key)
        dfin.index = index
        return(dfin.average.astype('float32'))

    def daily_turnover(self, data, ndays=1, useall=True):
        r"""
        Return the daily turnover over 'ndays' days.
        Arguments:
            data -- User data.
                    Required columns: [permno, 'col_date']
            col_date -- Column of the dates at which to compute the turnover.
            ndays --    Number of days to use to compute the turnover.
                        If positive, compute the turnover over
                        'ndays' in the future. If negative, compute the
                        turnover over abs(ndays) in the past.
            useall --  If True, use the turnover of the last
                        available trading date (if ndays<0) or the
                        turnover of the next available trading day
                        (if ndays>0).
        """
        # Open the necessary data
        key = ['permno', 'date']
        fields = ['shrout', 'vol']
        # Note: The number of shares outstanding (shrout) is in thousands.
        # Note: The volume in the daily data is expressed in units of shares.
        dsf = self._get_fields_dsf(fields)
        # Create the time series index
        dsf = dsf.set_index('date')
        # Compute the average bid-ask spread
        dsf['vol_sh'] = dsf.vol / (dsf.shrout * 1000)
        window = abs(ndays)
        s = dsf.groupby('permno').rolling(window).vol_sh.mean()
        if ndays < 0:
            dsf['turnover'] = s.values
        else:
            dsf['turnover'] = s.shift(1-ndays).values
        dsf = dsf.reset_index()
        # Use the last or next trading day if requested
        cols_data = ['permno', self.w.col_date]
        dfu = self.w.open_data(data, cols_data)
        index = dfu.index
        # Shift a maximum of 6 days
        if useall is True:
            t = 'past'
            if ndays > 0:
                t = 'future'
            dfu['date'] = self._closest_trading_date(dfu[self.w.col_date], t=t)
        else:
            dfu['date'] = dfu[self.w.col_date]
        # Merge the cumulative return to the data
        dfin = dfu.merge(dsf[key+['turnover']], how='left', on=key)
        dfin.index = index
        return(dfin.turnover.astype('float32'))

    ##########################
    # Variables Computations #
    ##########################

    def _tso(self, data):
        """ Compute total share outstanding. """
        cols = ['permno', 'date', 'shrout', 'cfacshr']
        df = self.w.open_data(self.sf, cols)
        if self.freq == 'M':
            dt = pd.to_datetime(df.date).dt
            df['year'] = dt.year
            df['month'] = dt.month
            key = ['permno', 'year', 'month']
        else:
            key = ['permno', 'date']
        df['tso'] = df.shrout * df.cfacshr * 1000
        dfu = self.w.open_data(data, ['permno', self.w.col_date])
        if self.freq == 'M':
            dt = pd.to_datetime(dfu[self.w.col_date]).dt
            dfu['year'] = dt.year
            dfu['month'] = dt.month
        else:
            dfu['date'] = dfu[self.w.col_date]
        dfin = dfu.merge(df[key+['tso']], how='left', on=key)
        dfin.index = dfu.index
        return(dfin.tso)
