"""
Provide merging and processing function for CRSP / Compustat
"""

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


def add_gvkey_from_permno(w, file_data, link_table):
    None


def get_permno_from_gvkey(w, file_data, link_table, col_date):
    # Columns required from data
    cols_data = ['gvkey', col_date]
    # Columns required from the link table
    cols_link = ['gvkey', 'lpermno', 'linktype', 'linkprim',
                 'linkdt', 'linkenddt']
    # Open the data
    data = w.open_data(file_data, cols_data)
    link = w.open_data(link_table, cols_link)
    # Check for duplicates for the user'sdata
    n_dup = data.shape[0] - data[cols_data].drop_duplicates().shape[0]
    if n_dup > 0:
        print("Warning: The data contains {:} duplicates".format(n_dup))
    # Keep non NA rows (except fr the link table 'linkenddt'
    data = data.dropna()
    link = link.dropna(subset=['gvkey', 'lpermno', 'linktype', 'linkprim'])
    # Correct the columns types
    data.gvkey = data.gvkey.astype(int)
    data[col_date] = data[col_date].astype('datetime64[ns]')
    link.gvkey = link.gvkey.astype(int)
    link.lpermno = link.lpermno.astype(int)
    link.linkdt = link.linkdt.astype('datetime64[ns]')
    link.linkenddt = link.linkenddt.astype('datetime64[ns]')
    # Retrieve the accurate links (LC, LU) for primary securities (P, C)
    link = link[(link.linktype.isin(['LC', 'LU'])) &
                (link.linkprim.isin(['P', 'C']))]
    link.drop(['linktype', 'linkprim'], 1, inplace=True)
    # Merge the data
    dm = data.merge(link, how='left', on='gvkey')
    # Filter the dates (keep correct matches)
    cond1 = (pd.notna(dm.linkenddt) &
             (dm[col_date] >= dm.linkdt) & (dm[col_date] <= dm.linkenddt))
    cond2 = (pd.isna(dm.linkenddt) & (dm[col_date] >= dm.linkdt))
    dm = dm[cond1 | cond2]
    # Rename lpermno to permno and remove unnecessary columns
    dm.rename(columns={'lpermno': 'permno'}, inplace=True)
    dm.drop(['linkdt', 'linkenddt'], 1, inplace=True)
    # Check for duplicates on the key
    n_dup = dm.shape[0] - dm[['gvkey', col_date]].drop_duplicates().shape[0]
    if n_dup > 0:
        print("Warning: The merged permno",
              "contains {:} duplicates".format(n_dup))
    return(dm)


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
    df_crsp['year'] = pd.DatetimeIndex(df_crsp['date']).year
    df_crsp['month'] = pd.DatetimeIndex(df_crsp['date']).month
    df_crsp['day'] = pd.DatetimeIndex(df_crsp['date']).day  # Daily data only

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
