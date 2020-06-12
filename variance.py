# This script provides an example on how to use the pywrd module to create
# a dataset for use in empirical tests

import pywrds as pw

# Create the wrds object and set the data directory to use
# The data directory is for new datasets creations
w = pw.wrds('datadir')

# We first need to convert the raw data into files that pywrds can use
# Define the files to use
raw_dsf = '~/Data/Databases/wrds/crsp/sasdata/a_stock/dsf.sas7bdat'
raw_link = '~/Data/Databases/wrds/crsp/sasdata/a_ccm/ccmxpf_lnkhist.sas7bdat'
data_var = '~/Dropbox/Research/Edwige/Edwige-Mario-Research/Variance/Tests/data/data_variance.csv'
# Convert the files (it wll automatically detect if the file has already been
# converted unless force=True is given as argument)
w.convert_data(raw_dsf)
w.convert_data(raw_link)
w.convert_data(data_var)

# The data can now be accessed using the filename prefix only
dsf = 'dsf'
link = 'ccmxpf_lnkhist'
variance = 'data_variance'

# Open the variance data
cols = ['gvkey', 'datadate', 'anndats']
dv = w.open_data(variance, cols)
# Add permno information to the variance data
permnos = pw.crspcomp.get_permno_from_gvkey(w, variance, link, 'datadate')
dv = dv.merge(permnos, how='left', on=['gvkey', 'datadate'])

# Add stock returns

# Save the data

# Get the compustat data that we need
cols_comp = ['gvkey', 'fyear', 'at', 'sic', 'naics']
dfcomp = pw.comp.get_naa_funda_us(w, funda, names, cols_comp)
