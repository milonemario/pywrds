"""
Provide merging and processing function for CRSP / Compustat
"""

# import pywrds as pw
import pandas as pd
# import datetime as dt

"Merge Compustat and CRSP data"

# CRSP data is daily or monthly.
# Compustat data is quarterly or annual.

# CCM: Link History Table (CCMXPF_LNKHIST)
# Link History Table (CCMXPF_LNKHIST) is Compustat-centric.
# Link with CRSP PERMNO and Compustat GVKEY (many to one).
# Link Type: LC and LU (most accurate links), LX and LD ("soft" links),
# NR and NU (no link available)

# GVKEY: integer, PK(1)
# LIID: char(3), PK(2)
# LINKDT: integer (date), PK(3)
# LINKDDT: integer (date)
# PERMNO: integer
# PERMCO: integer
# LINKPRIM: char(3)
# LINKTYPE: char(3)

# Type of links supported by the CRSP CCM link:
# 1. Find all securities in CRSP for Compustat
# 2. Find primary security in CRSP for Compustat Company data
# 3. Find data in CRSP for a specific Compustat Company and issue
# 4. Find Compustat data for a given CRSP security
# 5. Find Compustat company and security data for a CRSP security, only if it
#    is considered primary

# Variables:
# linkprim: P, J, C, M
# linktype: LC, LU, LX, LD, LS, LN, NR, NU
# star_date: yyyy/mm/dd
# end_date: yyyy/mm/dd (value is 99999999 if still effective)


def gvkey_from_permno(w, file_data, link_table):
    None


def permno_from_gvkey(w, data, link_table,
                      linktype=['LC', 'LU'], linkprim=['P', 'C']):
    """ Returns CRSP permno from COMPUSTAT gvkey.
    Arguments:
        data -- User provided data.
                Required columns: ['gvkey', 'datadate']
        link_table --   WRDS provided linktable (ccmxpf_lnkhist)
        linktype -- Default: [LC, LU]
        linkprim -- Default: [P, C]
    """
    index = data.index
    # Columns required from data
    key = ['gvkey', 'datadate']
    # Columns required from the link table
    cols_link = ['gvkey', 'lpermno', 'linktype', 'linkprim',
                 'linkdt', 'linkenddt']
    # Open the data
    data_key = w.open_data(data, key).drop_duplicates().dropna()
    link = w.open_data(link_table, cols_link)
    link = link.dropna(subset=['gvkey', 'lpermno', 'linktype', 'linkprim'])
    # Retrieve the specified links
    link = link[(link.linktype.isin(linktype)) &
                (link.linkprim.isin(linkprim))]
    link = link[['gvkey', 'lpermno', 'linkdt', 'linkenddt']]
    # Merge the data
    dm = data_key.merge(link, how='left', on='gvkey')
    # Filter the dates (keep correct matches)
    cond1 = (pd.notna(dm.linkenddt) &
             (dm['datadate'] >= dm.linkdt) & (dm['datadate'] <= dm.linkenddt))
    cond2 = (pd.isna(dm.linkenddt) & (dm['datadate'] >= dm.linkdt))
    dm = dm[cond1 | cond2]
    # Rename lpermno to permno and remove unnecessary columns
    dm = dm.rename(columns={'lpermno': 'permno'})
    dm = dm[['gvkey', 'datadate', 'permno']]
    # Check for duplicates on the key
    n_dup = dm.shape[0] - dm[key].drop_duplicates().shape[0]
    if n_dup > 0:
        print("Warning: The merged permno",
              "contains {:} duplicates".format(n_dup))
    # Add the permno to the user's data
    dfin = data[key].merge(dm, how='left', on=key)
    dfin.index = index
    return(dfin.permno)


def merge_crsp_compustat(w, file_crsp, file_comp, link_table, columns,
                         key=['gvkey', 'date', 'datadate']):

    # Solution 1: let end user input the frequency of imported data
    # Solution 2: check the frequency for imported data with code

    # Add the necessary columns to process the data
    # (permno, permco, link type, link primary)
    cols_req = key + ['permno', 'permco', 'lpermco', 'lpermno',
                      'linktype', 'linkprim']
    cols = cols_req + list(set(columns) - set(cols_req))

    # Open data
    df_crsp = w.open_data(file_crsp, cols)
    df_comp = w.open_data(file_comp, cols)
    df_lt = w.open_data(link_table, cols)

    # Extract year, month, day information
    df_crsp['year'] = df_crsp['date'].dt.year
    df_crsp['month'] = df_crsp['date'].dt.month
    df_crsp['day'] = df_crsp['date'].dt.day

    # Filter the linking table data
    df_lt = df_lt[(df_lt.linktype == 'LC' or 'LU') &
                  (df_lt.linkprim == 'P' or 'C')]

    # Add GVKEY to CRSP monthly data using the linking table
    df_lt_crsp = pd.merge(df_lt, df_crsp, left_on=['permco', 'permno'],
                          right_on=['lpermco', 'lpermno'])

    # Retain data within effective linking dates
    df_lt_crsp = df_lt_crsp[(df_lt_crsp.date >= df_lt_crsp.linkdt) and
                            (df_lt_crsp.date <= df_lt_crsp.linkenddt)]

    # Add the return from CRSP processed data
    # df_crsp = pd.merge(df_crsp, crsp_processed_ret, on=['permno','date']
    # df_crsp = pd.merge(df_crsp, crsp_processed_stats_dsf,
    #                    on=['permno','date'])

    # Drop useless daily statistics (too specific!!)
    # df_crsp.drop(columns = ['ret', 'vol', 'ask', 'bid', 'askhi', 'bidlo'])

    # Merge CRSP data with GVKEY with Compustat data
    df_ccm = pd.merge(df_comp, df_lt_crsp, how='right',
                      left_on=['gvkey', 'datadate'],
                      right_on=['gvkey', 'date'])

    # Check for duplicates on the key
    n_dup = df_ccm.shape[0] - df_ccm[key].drop_duplicates().shape[0]
    if n_dup > 0:
        print("Warning: The data contains {:} duplicates".format(n_dup))

    return df_ccm


def extract_ymd(df, date):
    # Put year, month, day information in separate columns
    df['year'] = df.date.dt.year
    df['month'] = df.date.dt.month
    df['day'] = df.date.dt.day

    return df


# Determine monthly or daily CRSP data
def check_monthly_or_daily(df_crsp):
    # Requires "permno" and "day" columns
    # Add first 5 values of "day"
    for i in range(0, 5):
        if df_crsp['permno'][i] != df_crsp['permno'][i+1]:
            # If permno are different, then the data is invalid.
            print("Provided data is not valid.")
            break
        else:
            x = df_crsp['day'][i+1] + df_crsp['day'][i]

    # Compare the sum of first 5 values of "day" to a fixed number(145)
    # For daily data, the maximum of the sum of 5 consecutive "day" values
    # is 31+30+29+28+27=145.
    # For monthly data, The sum of 5 consecutive values is always
    # greater than 145.
    if df_crsp['permno'][i] == df_crsp['permno'][i+1]:
        if x > 145:
            # Monthly if sum is greater than 145
            print('month')
        else:
            # Daily if sum is not greater than 145
            print('daily')
    else:
        # If permno are different, do nothing.
        None


def check_annual_or_quarterly(df_comp):
    # Determine annual or quarterly Compustat data
    # Requires "GVKEY" and "year" columns
    # Compare 1st and 5th value of "year"
    df_comp['year'] = df_comp.datadate.dt.year  # Extract year information

    if df_comp['gvkey'][0] == df_comp['gvkey'][5]:
        if df_comp['year'][0] == df_comp['year'][5]:
            # Quarterly if 1st value matches 5th value
            print('quarterly')
        else:
            # Annual if 1st value does not match 5th value
            print('annual')
    else:
        # If GVKEY of 1st and 5th row are different, then the data is invalid.
        print("Provided data is not valid.")
