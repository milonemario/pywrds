"""
Provide merging and processing function for CRSP / Compustat
"""
import pywrds as pw
import pandas as pd
import numpy as np
import datetime

w = pw.wrds('D:/finaldata')

# Define the files #
raw_msf = 'D:/wrds/crsp/sasdata/a_stock/msf.sas7bdat'
raw_dsf = 'D:/wrds/crsp/sasdata/a_stock/dsf.sas7bdat'
raw_lnkhist = 'D:/wrds/crsp/sasdata/a_ccm/ccmxpf_lnkhist.sas7bdat'
# Could not find these files!!!
### crsp_processed_ret_msf = 
### crsp_processed_stats_dsf =


# Convert the files #
w.convert_data(raw_msf)
w.convert_data(raw_dsf)
w.convert_data(raw_lnkhist)
### w.convert_data(crsp_processed_ret_msf)
### w.convert_data(crsp_processed_stats_dsf)

# The data can now be accessed using the filename prefix only #
msf = 'msf'
dsf = 'dsf'
lnkhist = 'lnkhist'
### crsp_processed_ret_msf = 'crsp_processed_ret_msf'
### crsp_processed_stats_dsf = 'crsp_processed_stats_dsf'

"""
"Add GVKEY to CRSP Monthly data"
- Add the gvkey information to crsp monthly data using the link table
"""
def get_gvkey(w, file_msf, file_linkingtable, columns, key=['gvkey', 'date']):
    # Return the consolidated financial reports of US domestic industrial
    # companies with standard accounting format.
    # Add the necessary columns to process the data
    cols_req = key + ['linktype', 'linkprim']
    cols = cols_req + list(set(columns) - set(cols_req))
    # Open funda data
    df = w.open_data(file_msf, cols)
    # Open the identifiers (names)
    dn = w.open_data(file_linkingtable, cols)
    # Filter the funda data
    dn = dn[(dn.indfmt == 'INDL'|'LU') & (dn.linkprim == 'P', 'C')]
    
    # Not sure about this part!!!
    ### & pd.notnull(dn['linkenddt']) = True
    ### df['date'] >= dn['linkdt'] & df['date'] <= dn['linkenddt']
	### df['date'] >= dn['linkdt'] 

    # Merge the names informations
    df = pd.merge(df, dn, on=['permco','permno')

    # Check for duplicates on the key
    n_dup = df.shape[0] - df[key].drop_duplicates().shape[0]
    if n_dup > 0:
        print("Warning: The data contains {:} duplicates".format(n_dup))
    return df

# Add GVKEY to CRSP monthly data
cols_comp = ['permco', 'permno']
msf_gvkey = get_gvkey(w, msf, lnkhist, cols_comp)

"""
"Add GVKEY to CRSP Daily data"
- Add the gvkey information to crsp daily data using the link table
"""
# Add GVKEY to CRSP daily data
dsf_gvkey = get_gvkey(w, dsf, lnkhist, cols_comp)


"""
"Create monthly data based on monthly CRSP"
- Add the return info from CRSP processed monthly data 
- Add year, month and day information for future merge with Compustat
"""
# Extract year, month and day from 'date' in msf_gvkey
msf_gvkey['year'] = pd.DatetimeIndex(msf_gvkey['date']).year
msf_gvkey['month'] = pd.DatetimeIndex(msf_gvkey['date']).month
msf_gvkey['day'] = pd.DatetimeIndex(msf_gvkey['date']).day

# Create monthly data based on monthly CRSP
msf_gvkey_monthly = pd.merge(msf_gvkey, crsp_processed_ret_msf, on['permno','date'])

"""
"Create monthly data based on daily CRSP"
- Use monthly msf_gvkey to only keep one row per month.
- Remove now useless daily statistics: ret, vol, ask, bid, askhi, bidlo
"""
# Create monthly data based on daily CRSP
dsf_gvkey_monthly = pd.merge(msf_gvkey, crsp_processed_stats_dsf, on['permno','date'])

# Drop useless daily data
dsf_gvkey_monthly.drop(['ret', 'vol', 'ask', 'bid', 'askhi', 'bidlo'], axis=1)

"""
"Create daily data based on daily CRSP"
- Add the statistics info from CRSP processed daily data
- Add year, month and day information for future merge with Compustat
"""
dsf_gvkey['year'] = pd.DatetimeIndex(dsf_gvkey['date']).year
dsf_gvkey['month'] = pd.DatetimeIndex(dsf_gvkey['date']).month
dsf_gvkey['day'] = pd.DatetimeIndex(dsf_gvkey['date']).day

dsf_gvkey_daily = pd.merge(dsf_gvkey, crsp_processed_stats_dsf, on['permno','date'])


"Merge Compustat and CRSP data"

def merge_crsp_compustat(file_crsp, file_comp):
    None
