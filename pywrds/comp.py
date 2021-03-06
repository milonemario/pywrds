"""
Provides processing functions for Compustat data
"""

from pywrds import wrds_module
import pandas as pd


class comp(wrds_module):

    def __init__(self, w):
        wrds_module.__init__(self, w)
        # Initialize values
        self.col_id = 'gvkey'
        self.col_date = 'datadate'
        self.freq = None  # Data frequency
        # Filters
        self.indfmt = ['INDL']
        self.datafmt = ['STD']
        self.consol = ['C']
        self.popsrc = ['D']

    def set_frequency(self, frequency):
        if frequency in ['Quarterly', 'quarterly', 'Q', 'q']:
            self.freq = 'Q'
            self.fund = self.fundq
        elif frequency in ['Annual', 'annual', 'A', 'a', 'Y', 'y']:
            self.freq = 'A'
            self.fund = self.funda
        else:
            raise Exception('Compustat data frequency should by either',
                            'Annual or Quarterly')

    def _get_fund_fields(self, fields, data, lag=0):
        """ Returns the fields from COMPUSTAT.
        Arguments:
            data -- User provided data
                    Required columns: [gvkey, datadate]
            fields --   Fields from file_fund
        """
        key = ['gvkey', 'datadate']
        cols_req = key + ['indfmt', 'datafmt', 'consol', 'popsrc']
        data_key = self.w.open_data(data, key)
        index = data_key.index
        comp = self.w.open_data(self.fund,
                                key+fields+cols_req).drop_duplicates()
        # Filter the fund data
        comp = comp[comp.indfmt.isin(self.indfmt) &
                    comp.datafmt.isin(self.datafmt) &
                    comp.consol.isin(self.consol) &
                    comp.popsrc.isin(self.popsrc)]
        comp = comp[key+fields]
        # Remove duplicates
        dup = comp[key].duplicated(keep=False)
        nd0 = comp[~dup]
        dd = comp[dup][key+fields]
        # 1. When duplicates, keep the ones with the most information
        dd['n'] = dd.count(axis=1)
        maxn = dd.groupby(key).n.max().reset_index()
        maxn = maxn.rename({'n': 'maxn'}, axis=1)
        dd = dd.merge(maxn, how='left', on=key)
        dd = dd[dd.n == dd.maxn]
        dup = dd[key].duplicated(keep=False)
        nd1 = dd[~dup][key+fields]
        dd = dd[dup]
        # 2. Choose randomly
        dd['rn'] = dd.groupby(key).cumcount()
        dd = dd[dd.rn == 0]
        nd2 = dd[key+fields]
        # Concatenate the non duplicates
        nd = pd.concat([nd0, nd1, nd2]).sort_values(key)
        # Get the lags if asked for
        ndl = nd.groupby('gvkey')[fields].shift(lag)
        nd[fields] = ndl
        # Merge and return the fields
        dfin = data_key.merge(nd, how='left', on=key)
        dfin.index = index
        return(dfin[fields])

    def _get_names_fields(self, fields, data):
        """ Returns the fields from COMPUSTAT.
        Arguments:
            data -- User provided data
                    Required columns: [gvkey, datadate]
            fields --   Fields from file_names
        """
        key = ['gvkey']
        data_key = self.w.open_data(data, key)
        index = data_key.index
        comp = self.w.open_data(self.names, key+fields).drop_duplicates()
        # Note: thereare no duplicates in the name file
        # Merge and return the fields
        dfin = data_key.merge(comp, how='left', on=key)
        dfin.index = index
        return(dfin[fields])

    def get_fields(self, fields, data=None, lag=0):
        """ Returns the fields from COMPUSTAT.
        Arguments:
            fields --   Fields from file_fund
            data -- User provided data
                    Required columns: [gvkey, datadate]
                    If none given, returns the entire compustat with key.
                    Otherwise, return only the fields with the data index.
            lag --  If lag=n, returns the nth lag of the fields.
        """
        key = ['gvkey', 'datadate']
        # Determine the raw and additional fields
        cols_fund = self.w.get_fields_names(self.fund)
        cols_names = self.w.get_fields_names(self.names)
        fields = [f for f in fields if f not in key]
        # fields_toc = [f for f in fields if (f not in fund_raw and
        #                                     f not in names_raw)]
        # Keep the full compustat key (to correctly compute lags)
        df = self.w.open_data(self.fund, key).drop_duplicates()
        # Get additional fields first (to ensure overwritten fields)
        for f in fields:
            if hasattr(self, '_' + f):
                # print(f)
                fn = getattr(self, '_' + f)
                df[f] = fn(df)
        fields_add = df.columns
        fund_raw = [f for f in fields if (f in cols_fund and
                                          f not in fields_add)]
        names_raw = [f for f in fields if (f in cols_names and
                                           f not in fields_add)]
        # Get the raw fields for the fund file
        if len(fund_raw) > 0:
            df[fund_raw] = self._get_fund_fields(fund_raw, df)
        # Get the raw fields for the names file
        if len(names_raw) > 0:
            df[names_raw] = self._get_names_fields(names_raw, df)
        # Get the lags if asked for
        if len(fields) > 0:
            df = self.get_lag(df, lag)
            # dfl = df.groupby('gvkey')[fields].shift(lag)
            # df[fields] = dfl
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
            return(df[key+fields])

    ###################
    # General Methods #
    ###################

    def volatility(self, fields, offset, min_periods, data, lag=0):
        """ Compute the volatility (standard deviation) of given fields.
        Arguments:
            fields --   Fields for which to compute the volatility
            offset --   Maximum window of time to compute the volatility
                        Can be expressed in number of past observations or
                        in time ('365D' for 365 days).
            min_periods --  Minimum number of past observations to compute
                            the volatility (cannot be expressed in time).
            data -- User data. Required.
        """
        key = ['gvkey', 'datadate']
        df = self.get_fields(key+fields)
        # Index the data with datadate
        df = df.set_index('datadate')
        g = df.groupby('gvkey')[fields]
        # Compute the volatility
        std = g.rolling(offset, min_periods=min_periods).std(skipna=False)
        # Use float32 to save space
        std = std.astype('float32')
        std = std.reset_index()
        # Shift the values if lag required TODO
        if lag != 0:
            stdl = std.groupby('gvkey')[fields].shift(lag)
            std[fields] = stdl
        # Merge to the data
        dfu = self.w.open_data(data, key)
        dfin = dfu.merge(std[key+fields], how='left', on=key)
        dfin.index = dfu.index
        return(dfin[fields])

    ##########################
    # Variables Computations #
    ##########################

    def add_var(self, fn, fields):
        def _var(data):
            key = ['gvkey', 'datadate']
            df = self.get_fields(key+fields)
            df['v'] = fn(df)
            # Merge to the data
            dfu = self.w.open_data(data, key)
            dfin = dfu.merge(df[key+['v']], how='left', on=key)
            dfin.index = dfu.index
            return(dfin.v)
        setattr(self, fn.__name__, _var)

    def _blevq(self, data):
        """ Return Book Leverage (Quarterly). """
        key = ['gvkey', 'datadate']
        df = self.w.open_data(data, key)
        fields = ['ltq', 'txdbq', 'atq']
        df[fields] = self.get_fields(fields, data)
        # Replace txdbq by 0 when missing
        df.loc[df.txdbq.isna(), 'txdbq'] = 0
        df['blevq'] = (df.ltq - df.txdbq) / df.atq
        return(df.blevq)

    def _capxq(self, data):
        """ Capital Expenditure (Quarterly). """
        key = ['gvkey', 'datadate']
        fields = ['fyr', 'capxy']
        df = self.get_fields(fields)
        df['l1capxy'] = self.get_fields(['capxy'], df, lag=1)
        df.loc[df.capxy.isna(), 'capxy'] = 0
        df.loc[df.l1capxy.isna(), 'l1capxy'] = 0
        df['capxq'] = df.capxy - df.l1capxy
        df['month'] = pd.DatetimeIndex(df.datadate).month
        df['monthQ1'] = (df.fyr + 2) % 12 + 1
        condQ1 = df.month == df.monthQ1
        df.loc[condQ1, 'capxq'] = df[condQ1].capxy
        # Merge to the data
        dfu = self.w.open_data(data, key)
        dfin = dfu.merge(df[key+['capxq']], how='left', on=key)
        dfin.index = dfu.index
        # Return the field
        return(dfin.capxq)

    def _dvtq(self, data):
        """ Return quarterly dividends.
        The quarterly dividend is the difference dvy[t]-dvy[t-1] except on
        the first quarter of the fiscal year where it is dvy[t].
        """
        key = ['gvkey', 'datadate']
        fields = ['fyr', 'dvy']
        df = self.get_fields(fields)
        df['l1dvy'] = self.get_fields(['dvy'], df, lag=1)
        df.loc[df.dvy.isna(), 'dvy'] = 0
        df.loc[df.l1dvy.isna(), 'l1dvy'] = 0
        df['dvtq'] = df.dvy - df.l1dvy
        df['month'] = pd.DatetimeIndex(df.datadate).month
        df['monthQ1'] = (df.fyr + 2) % 12 + 1
        condQ1 = df.month == df.monthQ1
        df.loc[condQ1, 'dvtq'] = df[condQ1].dvy
        # Merge to the data
        dfu = self.w.open_data(data, key)
        dfin = dfu.merge(df[key+['dvtq']], how='left', on=key)
        dfin.index = dfu.index
        # Return the field
        return(dfin.dvtq)

    def _epsd(self, data):
        """ Returns Diluted EPS. """
        key = ['gvkey', 'datadate']
        df = self.w.open_data(data, key)
        fields = ['ibq', 'cshfdq']
        df[fields] = self.get_fields(fields, data)
        df['epsd'] = df.ibq / df.cshfdq
        return(df.epsd)

    def _epsb(self, data):
        """ Returns Basic EPS (non-diluted). """
        key = ['gvkey', 'datadate']
        df = self.w.open_data(data, key)
        fields = ['ibq', 'cshprq']
        df[fields] = self.get_fields(fields, data)
        df['epsb'] = df.ibq / df.cshprq
        return(df.epsb)

    def _eqq(self, data):
        """ Return Operating Accruals (Quarterly). """
        key = ['gvkey', 'datadate']
        df = self.w.open_data(data, key)
        fields = ['prccq', 'cshoq']
        df[fields] = self.get_fields(fields, data)
        df['eqq'] = df.prccq * df.cshoq
        return(df.eqq)

    def _hhiq(self, data):
        """ Return Herfindahl-Hirschman Index (Quarterly). """
        key = ['gvkey', 'datadate']
        fields = ['revtq', 'sic']
        df = self.get_fields(key+fields).dropna()
        gk = ['sic', 'datadate']
        # Compute market share
        df['revtqsum'] = df.groupby(gk).revtq.transform('sum')
        # ms = df.groupby(gk).revtq.sum().reset_index(name='revtqsum')
        # df = df.merge(ms, how='left', on=gk)
        df['s'] = (df.revtq / df.revtqsum) * 100
        # Compute HHI index
        df['s2'] = df.s**2
        df['hhiq'] = df.groupby(gk).s2.transform('sum')
        # hhi = df.groupby(gk).s2.sum().reset_index(name='hhiq')
        # df = df.merge(hhi, how='left', on=gk)
        # Merge to the data
        dfu = self.w.open_data(data, key)
        dfin = dfu.merge(df[key+['hhiq']], how='left', on=key)
        dfin.index = dfu.index
        return(df.hhiq)

    def _litig(self, data):
        """ Litigation dummy.
        = 1 if SIC in [2833–2836, 8731–8734, 3570–3577, 3600–3674, 7371–7379,
                       5200–5961, 4812–4813, 4833, 4841, 4899, 4911, 4922–4924,
                       4931, 4941]
        """
        key = ['gvkey', 'datadate']
        df = self.w.open_data(data, key)
        fields = ['sic']
        df[fields] = self.get_fields(fields, data)
        ranges = [range(2833, 2837), range(8731, 8735), range(3570, 3578),
                  range(3600, 3675), range(7371, 7380), range(5200, 5962),
                  range(4812, 4814), range(4833, 4834), range(4841, 4842),
                  range(4899, 4900), range(4911, 4912), range(4922, 4925),
                  range(4931, 4932), range(4941, 4942)]
        rs = []
        for r in ranges:
            for i in r:
                rs = rs + [i]
        df['litig'] = 0
        df.loc[df.sic.isin(rs), 'litig'] = 1
        return(df.litig)

    def _mb0q(self, data):
        """ Return Market-to-Book ratio 0 (Quarterly). """
        key = ['gvkey', 'datadate']
        df = self.w.open_data(data, key)
        fields = ['eqq', 'ceqq']
        df[fields] = self.get_fields(fields, data)
        df['mb0q'] = df.eqq / df.ceqq
        return(df.mb0q)

    def _mroq(self, data):
        """ Return Operating margin (Quarterly). """
        key = ['gvkey', 'datadate']
        df = self.w.open_data(data, key)
        fields = ['revtq', 'xoprq']
        df[fields] = self.get_fields(fields, data)
        df['mroq'] = df.revtq / df.xoprq
        return(df.mroq)

    def _numfq(self, data):
        """ Return the number of firm by industry (SIC) (Quarterly). """
        key = ['gvkey', 'datadate']
        fields = ['sic']
        df = self.get_fields(key+fields).dropna()
        gk = ['sic', 'datadate']
        # Compute the number of gvkey by (sic, quarter)
        df['numfq'] = df.groupby(gk).gvkey.transform('count')
        # ms = df.groupby(gk).gvkey.count().reset_index(name='numfq')
        # df = df.merge(ms, how='left', on=gk)
        # Merge to the data
        dfu = self.w.open_data(data, key)
        dfin = dfu.merge(df[key+['numfq']], how='left', on=key)
        dfin.index = dfu.index
        return(df.numfq)

    def _oaccq(self, data):
        """ Return Operating Accruals (Quarterly). """
        key = ['gvkey', 'datadate']
        df = self.w.open_data(data, key)
        fields = ['actq', 'cheq', 'lctq', 'dlcq']
        l1f = ['l1'+f for f in fields]
        df[fields] = self.get_fields(fields, data)
        df[l1f] = self.get_fields(fields, data, lag=1)
        df['oacc'] = ((df.actq - df.cheq) - (df.l1actq - df.l1cheq) -
                      ((df.lctq - df.dlcq) - (df.l1lctq - df.l1dlcq)))
        return(df.oacc)

    def _roa0q(self, data):
        """ Return Return on Assets 0 (Quarterly). """
        key = ['gvkey', 'datadate']
        df = self.w.open_data(data, key)
        fields = ['niq', 'atq']
        df[fields] = self.get_fields(fields, data)
        df['roa0q'] = df.niq / df.atq
        return(df.roa0q)

    def _xrdq(self, data):
        """ Return Expenses in R&D with 0 when missing values (Quarterly). """
        key = ['gvkey', 'datadate']
        df = self.w.open_data(data, key)
        fields = ['xrdq']
        df[fields] = self._get_fund_fields(fields, df)  # Need the raw xrdq
        df.loc[df.xrdq.isna(), 'xrdq'] = 0
        return(df.xrdq)
