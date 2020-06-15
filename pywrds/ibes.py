"""
Provides processing functions for IBES data
"""


def _adjustmentfactors(w, file_adj):
    """ Create an adjustment factor table with a start and end date
    for each factor."""
    None


def consensus(w, file_det, file_adjfactors=None, file_adj=None):
    """Compute the consensus from individual analysts estimates."""
    # Open the det file with the necessary fields
    cols = ['itic']
    det = w.open_data(file_det, cols)
    # Opent the adjustment factors file
    if file_adjfactors is not None:
        adjfactors = w.open_data(file_adjfactors)
    elif file_adj is not None:
        # Need to compute the adustment factors
        adjfactors = _adjustmentfactors(file_adj)
    elif file_adj is None:
        raise Exception("Please provide either the adjustment factor file",
                        " or the adjustment file from IBES.")


def get_a_forecast(file, columns, key=['ticker']):
    """Fetch analysts forecasts"""
    # Add the necessary columns to process the data
    cols_req = key + ['usfirm', 'curr_act', 'measure', 'actual', 'fpi']
    cols = cols_req + list(set(columns) - set(cols_req))
    # Open ibes data
    df = w.open_data(file, cols)
    # Filter the ibes data
    df = df[(df['usfirm'] == 1) & (df['curr_act'] == 'USD') &
            (df['measure'] == 'EPS') & (df['fpi'].isin(['6', '7', '8', '9', 'N',
                                                        'O', 'P', 'Q', 'R', 'S',
                                                        'T', 'L', 'Y']))]
    # Drop if 'actual' is null
    df = df.dropna(subset=['actual'])
    # Extract year and month information from 'fpedats'
    df['fpeyr'] = df['fpedats'].dt.year
    df['fpemon'] = df['fpedats'].dt.month
    # Convert 'anntims' and 'anntims_act' to seconds format
    df['esttims'] = pd.to_datetime(df['anntims'], unit='s').dt.time
    df['anntims'] = pd.to_datetime(df['anntims_act'], unit='s').dt.time
    # Check for duplicates on the key
    n_dup = df.shape[0] - df[key].drop_duplicates().shape[0]
    if n_dup > 0:
        print("Warning: The data contains {:} duplicates".format(n_dup))

    return df


def get_m_forecast(file, columns, key=['ticker']):
    """Fetch the management forecast data"""
    # Add the necessary columns to process the data
    cols_req = key + ['usfirm', 'curr', 'pdicity', 'measure', 'units']
    cols = cols_req + list(set(columns) - set(cols_req))
    # Open ibes data
    df = w.open_data(file, cols)
    # Filter the ibes data
    df = df[(df.usfirm == 1) & (df.curr == 'USD') &
            (df.pdicity == 'QTR') & (df.measure == 'EPS') & (df.units == 'P/S')]
    # Convert 'anntims' to seconds format
    df['mftims'] = pd.to_datetime(df['anntims'], unit='s').dt.time
    # Check for duplicates on the key
    n_dup = df.shape[0] - df[key].drop_duplicates().shape[0]
    if n_dup > 0:
        print("Warning: The data contains {:} duplicates".format(n_dup))

    return df


def adjustments(file, columns, key=['ticker']):
    """Fetch the adjustment factors"""
    # Add the necessary columns to process the data
    cols_req = key + ['usfirm']
    cols = cols_req + list(set(columns) - set(cols_req))
    # Open ibes data
    df = w.open_data(file, cols)
    # Filter the ibes data
    df = df[(df.usfirm == 1)]
    # Check for duplicates on the key
    n_dup = df.shape[0] - df[key].drop_duplicates().shape[0]
    if n_dup > 0:
        print("Warning: The data contains {:} duplicates".format(n_dup))
    return df


def numanalys(file, columns, key=['ticker']):
    """Return the number of analysts for each (itic, fpedats)."""
    # Open ibes data
    df = w.open_data(file)
    # Number of distinct analysts by firm-forecast period
    df = df.groupby(['ticker', 'fpedats'])['analys'].nunique().reset_index()
    df.rename(columns={'analys': 'numanalys'}, inplace=True)

    return df


def itic_split(file, columns, n):
    """Select the firms that have at least n split"""
    df = df.groupby(['ticker'])['ticker'].count().to_frame('ticker_counts')
    df = df[(df['ticker_counts'] >= n)]

    return df


def adj1(file, columns, key=['ticker']):
    """Create the new adjustment table  """
    # Add the necessary columns to process the data
    cols_req = key + ['spdates']
    cols = cols_req + list(set(columns) - set(cols_req))
    # Open data
    df = w.open_data(file)
    # Sort values
    df = df.sort_values(by=['ticker', 'spdates'],
                        ascending=[1, 0]).reset_index()
    # Create ticker list
    tickers = df['ticker'].unique()
    ticker_list = df['ticker'].to_list()
    # Get index of each ticker from ticker list
    l_index = []
    for i in range(0, len(tickers)):
        x = ticker_list.index(tickers[i])
        l_index.append(x)
    # Create empty column 'enddt'
    df['enddt'] = ''
    # Set values to 0 for each ticker from list if 1st occurence
    for i in l_index:
        df['enddt'][i] = 0
    # Store values into 'enddt' based on condition
    for i in range(0, len(df)):
        if df['enddt'][i] == 0:
            continue
        else:
            df['enddt'][i] = df['spdates'][i - 1]
    # Drop if 'adj' = 1
    df = df.drop[(df['adj'] == 1)]

    return df


def _adjustmentfactors(file, columns, key=['ticker']):
    """Create an adjustment factor table with a start and end date for each
    factor. """
    # Add the necessary columns to process the data
    cols_req = key + ['spdates']
    cols = cols_req + list(set(columns) - set(cols_req))
    # Open ibes data
    df = w.open_data(file)
    # Sort values
    df = df.sort_values(by=['ticker', 'spdates'],
                        ascending=[1, 1]).reset_index()
    # Create ticker list
    tickers = df['ticker'].unique()
    ticker_list = df['ticker'].to_list()
    # Get index of each ticker from ticker list
    l_index = []
    for i in range(0, len(tickers)):
        x = ticker_list.index(tickers[i])
        l_index.append(x)
    # Create empty column 'startdt'
    df['startdt'] = ''
    # Set values to 0 for each ticker from list if 1st occurence
    for i in l_index:
        df['startdt'][i] = 0
    # Store values into 'startdt' based on condition
    for i in range(0, len(df)):
        if df['startdt'][i] == 0:
            continue
        else:
            df['startdt'][i] = df['spdates'][i]

    return df


def _a2(df):
    """Keep the diluted EPS analysts estimates or the primary if no diluted
    available """
    # Create _id_p
    # df = df.groupby(['ticker','fpedats'])['pdf'].max().reset_index()
    # df = df[df['pdf1'] == 'P']

    # Create _a1
    df = df[df['pdf'] == 'D']
    # Drop when 'anndats' is null
    df = df.dropna(subset=['anndats'])
    # Remove observations where the period between FPE and EA is more than
    # 120 days and that do not have EA date.
    df['interval'] = (df['anndats'] - df['fpedats'])
    df = df[df['interval'] / np.timedelta64(1, 'D') <= 120]

    return df


"""Get the last 2 earnings annoucement dates"""


def _ann1(df):
    # Sort data
    df = df.groupby(['ticker', 'fpedats', 'anndats'])[
        'analys'].nunique().reset_index()
    df = df.sort_values(by=['ticker', 'fpedats'],
                        ascending=[1, 1]).reset_index()
    # Create empty column 'anndats_l'
    df['anndats_l'] = ''
    # Store values
    for i in range(1, len(df)):
        df['anndats_l'][i] = df['anndats'][i - 1]
    # Convert 'anndats_l' format to datetime days
    df['anndats_l'] = pd.to_datetime(df['anndats_l']).dt.floor('d')

    return df


def _ann2(df):
    # Create temporary column 'interval'
    df['interval'] = ''
    # Calculate difference and store values
    df['interval'] = df['anndats'] - df['anndats_l']
    df['interval'] = df[df['interval'] / np.timedelta64(1, 'D') <= 120]

    #    for i in range(0,len(df)):
    #       if df['interval'][i] < 120:
    #            continue
    #       else:
    #           df['anndats_l'][i] = df['anndats'][i] - 120

    # Drop temporary column 'interval'
    df.drop(columns=['interval'])

    return df


def _ann3(df):
    # Sort data
    df = df.sort_values(by=['ticker', 'fpedats'],
                        ascending=[1, 1]).reset_index()
    # Create empty column 'anndats_ll'
    df['anndats_ll'] = ''
    # Store values
    for i in range(1, len(df)):
        df['anndats_ll'][i] = df['anndats_l'][i - 1]

    return df


def _ann4(df):
    # Create temporary column 'interval'
    df['interval'] = df['anndats_l'] - df['anndats_ll']
    df['interval'] = df[df['interval'] / np.timedelta64(1, 'D')]

    #   for i in range(0,len(df)):
    #       if df['interval'][i]<120:
    #           continue
    #       else:
    #           df['anndats_ll'][i] = df['anndats_l'][i] - 120

    # Drop temporary column 'interval'
    df.drop(columns=['interval'])

    return df


def _a3(df_a2, df_ann4):
    """Merge with analysts estimates"""
    df = pd.merge(d1, d2, on=['ticker', 'fpedats'])

    return df


"""Keep MF(t) between EA(t-1) and FPE(t) Note that the window between the
    annoucements is only defined with the dates to loose fewer observations
    (suspecting timestamps problems)"""


def _mf1(df_m_forecast, df_a3):
    df = pd.merge(d1, d2, on=['ticker', 'fpeyr', 'fpemon'])

    df.drop_duplicates(
        subset=['ticker', 'fpeyr', 'fpemon', 'anndats_l', 'fpedats'])

    return df


def _mf2(df_mf1):
    df = df[(df['mfdats'] >= df['anndats_l']) & (df['mfdats'] <= df['fpedats'])]

    df = df.sort_values(by=['ticker', 'fpeyr', 'fpemon', 'mfdats'],
                        ascending=[1, 1, 1, 1]).reset_index()

    df.drop_duplicates(subset=['ticker', 'fpeyr', 'fpemon'])

    return df


def _amf1(df_a3, df_mf2):
    """Merge the analysts estimates and management forecast data"""
    df = pd.merge(df_a3, df_mf2,
                  on=['ticker', 'fpedats', 'fpeyr', 'fpemon', 'anndats_l'])

    return df


def _amf2(df_amf1, df_adj_factors):
    """Merge the adjustement factors"""
    df = pd.merge(df_a3, df_mf2, on='ticker')

    #   if df['startdt'] == 0:
    #       df['fpedats'] < df['enddt']
    #   else:
    #       df['fpedats'] >= df['startdt']
    #       df['fpedats'] < df['enddt']
    # Complete the missing ones
    df['adj'] = df['adj'].fillna(1)

    return df


def _amf3(df):
    """Compute the unajusted estimates and eps"""
    df['uest'] = df['est'] * df['adj']
    df['ueps'] = df['eps'] * df['adj']

    return df


"""Find the Ex-Post and Ex-ante consensus depending on disclosure or 
no-disclosure"""


def _expd_est(df):
    """Ex-Post consensus when disclosure"""
    df = df.dropna(subset=['mfdats'])
    df = df[(df['estdats'] >= df['mfdats']) & (df['estdats'] <= df['fpedats'])]

    df['temp'] = df['estdats'] + df['esttims']

    df = df.sort_values(by=['ticker', 'fpedats', 'temp'],
                        ascending=[1, 1, 0]).reset_index()
    #   n<=5

    return df


def _expd(df, n):
    """Compute the unadjusted consensus"""
    n = 0.5
    df['meanest'] = df['uest'].mean()
    #   df['medest'] =
    #   df.groupby['ticker','fpedats']

    return df


def _expnd_est():
    # Similar to _expd_est, possible to create one function
    None


def _expnd():
    # Similar to _expd, possible to create one function
    None


def _exp(df_expd, df_expnd):
    """Merge ex-post disclosures"""
    df = pd.merge(df_expd, df_expnd, how='inner')

    return df


def _exa1_est():
    """Ex-Ante disclosure number 1: when MF(t) > EA(t-1)"""
    # Similar to _expd_est, possible to create one function
    None


def _exa1():
    # Similar to _expd, possible to create one function
    None


def _exa2_est():
    # Similar to _expd_est, possible to create one function
    None


def _exa2():
    # Similar to _expd, possible to create one function
    None


def ibes_consensus(df_amf3, df_exp, df_exa1, df_exa2):
    """Merge all"""
    # Rounding

    # Merge datasets on 'ticker' and 'fpedats'
    df = pd.merge(df_amf3, df_exp, on=['ticker', 'fpedats'])
    df = pd.merge(df, df_exa1, on=['ticker', 'fpedats'])
    df = pd.merge(df, df_exa2, on=['ticker', 'fpedats'])
    # Retain unique values
    df.drop_duplicates(
        subset=['ticker', 'fpedats', 'fpeyr', 'fpemon', 'anndats', 'mfdats',
                'eps,val_1', 'val_2', 'mean_at_date'])

    return df


def merge_consensus_stats(w, file_ibes_consesus, file_ibes_stats, columns):
    """Merge the consensus and the statistics"""
    # Add the necessary columns to process the data
    cols_req = ['itic', 'fpedats']
    cols = cols_req + list(set(columns) - set(cols_req))

    # Open data
    df_ibes_consesus = w.open_data(file_ibes_consesus, cols)
    df_ibes_stats = w.open_data(file_ibes_stats, cols)

    # Merge the two datasets on 'itic' and 'fpedats'
    df_ibes = pd.merge(df_ibes_consesus, df_ibes_stats, on=['itic', 'fpedats'])

    return df_ibes
