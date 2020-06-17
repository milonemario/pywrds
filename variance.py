# This script provides an example on how to use the pywrd module to create
# a dataset for use in empirical tests

import pywrds as pw
# import pyarrow as pa
# import pyarrow.parquet as pq

# Create the wrds object and set the data directory to use
# The data directory is for new datasets creations
w = pw.wrds('datadir')

# We first need to convert the raw data into files that pywrds can use
# Define the files to use
raw_dsf = '~/Data/Databases/wrds/crsp/sasdata/a_stock/dsf.sas7bdat'
raw_link = '~/Data/Databases/wrds/crsp/sasdata/a_ccm/ccmxpf_lnkhist.sas7bdat'
data_var = ('~/Dropbox/Research/Edwige/Edwige-Mario-Research/'
            'Variance/Tests/data/data_variance.csv')
# File to save the new data
data_new = ('~/Dropbox/Research/Edwige/Edwige-Mario-Research/'
            'Variance/Tests/data/data_variance2.csv')
data_parquet = ('~/Dropbox/Research/Edwige/Edwige-Mario-Research/'
                'Variance/Tests/data/data_variance2.parquet')
# data_parquet = 'data_variance2.parquet'
# Convert the files (it wll automatically detect if the file has already been
# converted unless force=True is given as argument)
w.convert_data(raw_dsf)
w.convert_data(raw_link)
w.convert_data(data_var)

# The data can now be accessed using the filename prefix only
dsf = 'dsf'
link = 'ccmxpf_lnkhist'
variance = 'data_variance'

# Open the variance data and correct the columns types
# cols = ['gvkey', 'datadate', 'anndats']
# dv = w.open_data(variance, cols)
print('Open the Variance file')
dv = w.open_data(variance)
dv = pw.correct_columns_types(dv)

# Add permno information to the variance data
print('Add permno information')
permnos = pw.crspcomp.get_permno_from_gvkey(w, variance, link, 'datadate')
dv = dv.merge(permnos, how='left', on=['gvkey', 'datadate'])

# Add stock returns
print('Add stock return information')
dv_ret1 = pw.crsp.daily_returns(w, dv, dsf, 'anndats', 1)
dv['anndats_ret'] = dv_ret1

# Save the data
print('Save the data')
dv.to_parquet(data_parquet, compression=None, index=False)
# dv.to_csv(data_new, index=False)
# dv.to_feather(data_new)
# table = pa.Table.from_pandas(dv, preserve_index=False)
# pq.write_table(table, data_parquet, compression='none')
