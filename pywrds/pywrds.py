"""
Python module to process WRDS data
"""

# import sys
import os
import pathlib
# import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class wrds():

    def __init__(self, datadir=None):
        self.datadir = None
        if datadir is not None:
            self.set_data_directory(datadir)
        self.chunksize = 10000

    def _check_data_dir(self):
        if self.datadir is None:
            raise Exception("Please provide a data directory with",
                            "'set_data_directory()'")

    def set_chunksize(self, size):
        self.chunksize = size

    def set_data_directory(self, datadir):
        # Defines the temporary data directory to store intermediate datasets
        if datadir[-1] != '/':
            self.datadir = datadir+'/'
        else:
            self.datadir = datadir
        # Create the directory if it does not exist
        pathlib.Path(datadir).mkdir(parents=True, exist_ok=True)

    def convert_data(self, filename, force=False):
        # Convert the given file to arrow and save it in the data directory
        self._check_data_dir()
        # Get the file type
        name, ext = os.path.splitext(os.path.basename(filename))
        # Create the new file name
        filename_pq = self.datadir+name+'.parquet'
        # Check if the fle has already been converted
        if os.path.exists(filename_pq) and force is False:
            print("The file has already been converted. " +
                  "Use force=True to force the conversion.")
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
                raise Exception("This file format is not currently",
                                "supported. Supported formats are:",
                                ".sas7bdat, .csv")
            # Write the data
            pqwriter = None
            pqschema = None
            for i, df in enumerate(f):
                df = self._process_fields(df)
                df.columns = map(str.lower, df.columns)  # Lower case col names
                print("Progress conversion {}: {:2.0%}".format(name,
                      (i+1)*f.chunksize/float(nrows)), end='\r')
                if i == 0:
                    t = pa.Table.from_pandas(df)
                    pqschema = t.schema
                    pqwriter = pq.ParquetWriter(filename_pq, t.schema)
                else:
                    t = pa.Table.from_pandas(df, schema=pqschema)
                pqwriter.write_table(t)
            pqwriter.close()

    def _process_fields(self, df):
        # Encode properly the string fields (remove bytes string types)
        for c in df.columns:
            if df[c].dtype == object:
                df[c] = df[c].where(df[c].apply(type) != bytes,
                                    df[c].str.decode('utf-8'))
        return df

    def open_data(self, name, columns=None):
        self._check_data_dir()
        # Open the parquet file and convert it to a pandas DataFrame
        filename_pq = self.datadir+name+'.parquet'
        t = pq.read_table(filename_pq, columns=columns)
        df = t.to_pandas()
        del(t)
        # Encode properly the string fields (remove bytes string types)
        # for c in df.columns:
        #    if df[c].dtype == object:
        #        df[c] = df[c].where(df[c].apply(type) != bytes,
        #                df[c].str.decode('utf-8'))
        df = df.drop_duplicates()
        return df
