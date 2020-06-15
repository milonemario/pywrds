# This script provides an example on how to use the pywrd module to create
# a dataset for use in empirical tests

import pywrds as pw

# Create the wrds object and set the data directory to use
# The data directory is for new datasets creations
w = pw.wrds('D:/finaldata')

# We first need to convert the raw data into files that pywrds can use
# Define the files to use
raw_funda = 'D:/wrds/comp/sasdata/naa/funda.sas7bdat'
raw_names = 'D:/Data/Databases/wrds/comp/sasdata/naa/names.sas7bdat'
# Convert the files (it wll automatically detect if the file has already been
# converted unless force=True is given as argument)
w.convert_data(raw_funda)
w.convert_data(raw_names)

# The data can now be accessed using the filename prefix only
funda = 'funda'
names = 'names'

# Get the compustat data that we need
cols_comp = ['gvkey', 'fyear', 'at', 'sic', 'naics']
dfcomp = pw.comp.get_naa_funda_us(w, funda, names, cols_comp)

print(dfcomp.head())
