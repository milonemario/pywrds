"""
Python module to process WRDS data
"""

# import sys
import os
import pathlib
import yaml
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import dask.dataframe as dd
import multiprocessing as mp
# Import pywrds modules
# from .comp import comp

# Remove warning messages for chained assignments
pd.options.mode.chained_assignment = None


class wrds:

    def __init__(self, datadir=None):
        self.datadir = None
        if datadir is not None:
            self.set_data_directory(datadir)
        self.chunksize = 10000
        self.cores = mp.cpu_count()
        # Add the different modules
        from .comp import comp
        self.comp = comp(self)
        from .crsp import crsp
        self.crsp = crsp(self)
        from .cboe import cboe
        self.cboe = cboe(self)
        from .tr import tr13f
        self.tr13f = tr13f(self)
        from .sec import sec13f
        self.sec13f = sec13f(self)
        from .ibes import ibes
        self.ibes = ibes(self)
        from .icc import icc
        self.icc = icc(self)
        from .fred import fred
        self.fred = fred(self)

    def _check_data_dir(self):
        if self.datadir is None:
            raise Exception("Please provide a data directory with",
                            "'set_data_directory()'")

    def set_chunksize(self, size):
        self.chunksize = size

    def set_data_directory(self, datadir):
        """ Defines the temporary data directory to store
        intermediate datasets.
        """
        if datadir[-1] != '/':
            self.datadir = datadir+'/'
        else:
            self.datadir = datadir
        # Create the directory if it does not exist
        pathlib.Path(datadir).mkdir(parents=True, exist_ok=True)

    def convert_data(self, filename, force=False, sample=None, types=None):
        """ Convert the given file to arrow and save it in the
        data directory.
        Arguments:
            filename -- Path of the file to convert.
            force --    If False, the file is not converted if already done.
            sample --   Integer. Only convert N observations.
        """
        self._check_data_dir()
        # Get the file type
        name, ext = os.path.splitext(os.path.basename(filename))
        # Create the new file name
        filename_pq = self.datadir+name+'.parquet'
        # Check if the fle has already been converted
        if os.path.exists(filename_pq) and force is False:
            # print("The file has already been converted. " +
            #       "Use force=True to force the conversion.")
            None
        else:
            # Open the file (by chunks)
            if ext == '.sas7bdat':
                f = pd.read_sas(filename, chunksize=self.chunksize)
                # Get the total number of rows
                nrows = f.row_count
            elif ext == '.csv':
                f = pd.read_csv(filename, chunksize=self.chunksize)
                # Get the total number of rows
                # Need to open the file (only one column)
                f_tmp = pd.read_csv(filename, usecols=[0])
                nrows = f_tmp.shape[0]
                del(f_tmp)
            else:
                raise Exception("This file format is not currently" +
                                "supported. Supported formats are:" +
                                ".sas7bdat, .csv")
            # Write the data
            pqwriter = None
            # pqschema = None
            for i, df in enumerate(f):
                df = self._process_fields(df)
                df.columns = map(str.lower, df.columns)  # Lower case col names
                df = correct_columns_types(df, types=types)
                nobs = (i+1)*f.chunksize
                print("Progress conversion {}: {:2.0%}".format(name,
                      nobs/float(nrows)), end='\r')
                if i == 0:
                    # Correct the column types on the first chunk to get
                    # the correct schema
                    t = pa.Table.from_pandas(df)
                    pqschema = t.schema
                    pqwriter = pq.ParquetWriter(filename_pq, t.schema)
                else:
                    t = pa.Table.from_pandas(df, schema=pqschema)
                    # t = pa.Table.from_pandas(df)
                pqwriter.write_table(t)
                if sample is not None and nobs > sample:
                    break
            print('\r')
            pqwriter.close()
        return(name)

    def _process_fields(self, df):
        """ Properly encode the string fields (remove bytes string types) """
        for c in df.columns:
            if df[c].dtype == object:
                df[c] = df[c].where(df[c].apply(type) != bytes,
                                    df[c].str.decode('utf-8', errors='ignore'))
        return df

    def open_data(self, name, columns=None, types=None, dask=False):
        """ Open the data and return a pandas DataFrame.
        If a DataFrame is given, return the specified columns.
        If a string is given, return the converted data with the
        specifid columns.
        """
        if isinstance(name, pd.DataFrame):
            # If the name refers to a pandas DataFrame, just return it
            df = name[columns]
            # df = correct_columns_types(df, types)
        else:
            # Otherwise, open the file from disk
            self._check_data_dir()
            # Open the parquet file and convert it to a pandas DataFrame
            filename_pq = self.datadir+name+'.parquet'
            if dask:
                df = dd.read_parquet(filename_pq, columns=columns,
                                     engine='pyarrow')
            else:
                t = pq.read_table(filename_pq, columns=columns)
                df = t.to_pandas()
                del(t)
        # df = df.drop_duplicates()
        return df

    def get_fields_names(self, name):
        if isinstance(name, pd.DataFrame):
            cols = name.columns
        else:
            # Otherwise, open the file from disk
            self._check_data_dir()
            # Open the parquet file and convert it to a pandas DataFrame
            filename_pq = self.datadir+name+'.parquet'
            schema = pq.read_schema(filename_pq)
            cols = schema.names
        return(cols)

    def set_data_frequency(self, frequency):
        """ Define the frequency of the user's data """
        if frequency in ['Daily', 'daily', 'D', 'd']:
            self.freq = 'D'
        elif frequency in ['Monthly', 'monthly', 'M', 'm']:
            self.freq = 'M'
        elif frequency in ['Quarterly', 'quarterly', 'Q', 'q']:
            self.freq = 'Q'
        elif frequency in ['Annual', 'annual', 'A', 'a',
                           'Yearly', 'yearly', 'Y', 'y']:
            self.data_freq = 'A'
        else:
            raise Exception('The data frequency must be either' +
                            ' Daily, Monthly, Quarterly, or Annual')

    def set_date_column(self, col_date):
        """ Define the date column to use for the user's data. """
        self.col_date = col_date

    def set_cores_no(self, cores):
        """ Set the number of cores to use.
        """
        self.cores = cores


class wrds_module:

    def __init__(self, w):
        # Link to the wrds object
        self.w = w

    def add_file(self, name, path, force=False):
        f = self.w.convert_data(path, force)
        setattr(self, name, f)

    def get_lag(self, data, lag, fields=None, col_id=None, col_date=None):
        """ Return the lag of the columns in the data (or the fields if
        specified).
        Arguments:
            data -- Data
            lag --  Number of lags
            fields -- Specific fields to return
            col_id --   Identifier column (gvkey, ticker, permno, ...)
                        If None, use self.col_id
            col_date -- Date column (datadate, fpedats, date, ...)
                        If None, use self.col_date
        """
        if col_id is None:
            col_id = self.col_id
        if col_date is None:
            col_date = self.col_date
        all_fields = [f for f in data.columns if f not in [col_id, col_date]]
        if lag != 0:
            data = data.sort_values([col_id, col_date])
            data_l = data.groupby(col_id)[all_fields].shift(lag)
            data[all_fields] = data_l
        if fields is None:
            return(data)
        else:
            return(data[fields])


####################
# Global functions #
####################


def check_duplicates(df, key, description):
    """ Check duplicates and print a message if needed. """
    n_dup = df.shape[0] - df[key].drop_duplicates().shape[0]
    if n_dup > 0:
        print("Warning: The {:} contains {:} \
              duplicates".format(description, n_dup))


def correct_columns_types(df, types=None):
    """ Apply the correct data type to all known columns.
    Known columns are listed in the files contained in the folder 'types'.
    A custom type file can be provided by the user.
    """
    types_dir = os.path.dirname(__file__)+'/types/'

    def get_changes(path):
        with open(path) as f:
            t = yaml.full_load(f)
            # Create the final type list
            types_list1 = {}  # Convert to float before Int64
            types_list2 = {}
            for k, l in t.items():
                for v in l:
                    types_list1[v] = k
                    types_list2[v] = k
                    if k in ['Int64', 'Int32']:
                        types_list1[v] = 'float'
        # Select the common columns
        cols_df = df.columns
        cols_change = [v for v in cols_df if v in types_list2.keys()]
        types_list_ch1 = {k: types_list1[k] for k in cols_change}
        types_list_ch2 = {k: types_list2[k] for k in cols_change
                          if types_list2[k] in ['Int64', 'Int32']}
        return([types_list_ch1, types_list_ch2])

    def apply_changes(ch):
        # Apply the date and non-date types separately
        chd = {k: v for k, v in ch.items() if v == 'date'}
        chnd = {k: v for k, v in ch.items() if v != 'date'}
        # Apply the non-dates
        if len(chnd) > 0:
            df.loc[:, chnd.keys()] = df[chnd.keys()].astype(chnd)
        # Apply the dates
        if len(chd) > 0:
            for k in chd.keys():
                df.loc[:, k] = pd.to_datetime(df[k]).dt.date
                df.loc[df[k].isna(), k] = np.nan


    # First use the predefined types
    for fname in os.listdir(types_dir):
        name, ext = os.path.splitext(fname)
        if ext == '.yaml':
            path = types_dir+fname
            changes = get_changes(path)
            for ch in changes:
                apply_changes(ch)
    # Then use the user provided types
    if types is not None:
        changes = get_changes(types)
        for ch in changes:
            apply_changes(ch)
    return(df)
