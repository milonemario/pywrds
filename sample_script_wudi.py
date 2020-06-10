import pywrds as pw
import pandas as pd

w = pw.wrds('D:/finaldata')

# Define the files #
raw_msf = 'D:/wrds/crsp/sasdata/a_stock/msf.sas7bdat'
raw_dsf = 'D:/wrds/crsp/sasdata/a_stock/dsf.sas7bdat'
raw_funda = 'D:/wrds/crsp/sasdata/naa/funda.sas7bdat'
raw_fundq = 'D:/wrds/crsp/sasdata/naa/fundq.sas7bdat'
raw_lnkhist = 'D:/wrds/crsp/sasdata/a_ccm/ccmxpf_lnkhist.sas7bdat'

# Convert the files #
w.convert_data(raw_msf)
w.convert_data(raw_dsf)
w.convert_data(raw_funda)
w.convert_data(raw_fundaq)
w.convert_data(raw_lnkhist)

# The data can now be accessed using the filename prefix only #
msf = 'msf'
dsf = 'dsf'
funda = 'funda'
fundq = 'fundq'
lnkhist = 'lnkhist'

# Get the data that we need
cols = ['gvkey', 'retx']
df_merged = pw.crspcomp.merge_crsp_compustat(w, msf, funda, m, a, cols)