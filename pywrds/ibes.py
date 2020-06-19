"""
Provides processing functions for IBES data
"""

import pandas as pd


def get_forecasts(w, file_det, at_date, for_quarter, min_date,
                  file_adjfactors=None, file_adj=None):
    """ Return analysts forecasts at a given date for a given quarter.
    Do not consider forecasts prior to min_date.
    """
    cols = ['ticker', 'analys', 'value', 'actual', 'anndats', 'anndats_act',
            'fpedats']
    det = w.open_data(file_det, cols)
    print(det[0:1000])
    None


def _adjustmentfactors(w, file_adj):
    """ Create an adjustment factor table with a start and end date
    for each factor."""
    # Add the necessary columns to process the data
    cols = ['ticker', 'spdates', 'adj']
    # Open adjustment factors data
    df_adj = w.open_data(file_adj, cols)
    # Select the firms that have at least one split
    df_ticker = df_adj.groupby(['ticker'])['ticker'].count(). \
        to_frame('ticker_counts')
    df_ticker = df_ticker[(df_ticker['ticker_counts'] > 1)].reset_index()
    ticker_list = df_ticker['ticker'].to_list()
    # Create the new adjustment table
    df_ticker = df_ticker.sort_values(by=['ticker', 'spdates'],
                                      ascending=[1, 0]).reset_index()
    tickers = df_adj['ticker'].unique()
    list_idx = []
    for i in range(0, len(tickers)):
        if tickers[i] in ticker_list:
            idx = ticker_list.index(tickers[i])
            list_idx.append(idx)
    df = pd.merge(df_ticker, df_adj, on='ticker')
    df['enddt'] = ''
    df['startdt'] = ''
    # Solution 1 (Set all dates first, then change first occurrences to 0)
    #   for i in range (1,len(df)):
    #        df['enddt'][i] = df['spdates'][i-1]
    #        df['startdt'][i] = df['spdates'][i]
    #   for i in l:
    #       df['enddt'][i] = 0
    #       df['startdt'][i] = 0
    # Solution 2 (Set first occurrences to 0, then set all other dates)
    # for i in l:
    #     df['enddt'][i] = 0
    #     df['startdt'][i] = 0
    for i in range(1, len(df)):
        if df['enddt'][i] == 0:
            continue
        else:
            df['enddt'][i] = df['spdates'][i - 1]
            df['startdt'][i] = df['spdates'][i]
    return df


# Create a statistics table that includes:
# -numanalys    Number of distinct analysts by firm-forecast period

def numanalys(w, file_det):
    """Return the number of analysts for each (itic, fpedats)."""
    # Open the det file with the necessary fields
    cols = ['ticker', 'fpedats', 'analys']
    det = w.open_data(file_det, cols)
    # Count number of analysis
    df = det.groupby(['ticker', 'fpedats'])['analys'].nunique().reset_index()
    df.rename(columns={'analys': 'numanalys'}, inplace=True)

    return df


def consensus(w, file_det, file_adjfactors=None, file_adj=None, start_date=[],
              end_date=[]):
    """Compute the consensus from individual analysts estimates."""
    # Open the det file with the necessary fields
    cols = ['ticker', 'usfirm', 'curr_act', 'measure', 'fpi', 'actual',
            'fpedats']
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

    """Fetch analysts forecasts"""
    # Keep quarterly (fpi={6,7,8}) diluted EPS (pdf=D)
    det = det[(det['usfirm'] == 1) & (det['curr_act'] == 'USD') &
              (det['measure'] == 'EPS') & (det['fpi'].isin(['6', '7', '8', '9',
                                                            'N', 'O', 'P', 'Q',
                                                            'R', 'S', 'T', 'L',
                                                            'Y']))]. \
        reset_index(drop=True)
    det = det.dropna(subset=['actual'])
    det['fpeyr'] = det['fpedats'].dt.year
    det['fpemon'] = det['fpedats'].dt.month
    # Rename dataframe to improve code readability
    df_afc = det

    """Fetch the adjustment factors"""
    # Filter the adjustment data
    adjfactors = adjfactors[(adjfactors['usfirm'] == 1)].reset_index(drop=True)
    # Rename dataframe to improve code readability
    df_adj = adjfactors

    df_afc = df_afc.sort_values(by=['ticker', 'fpedats'],
                                ascending=[1, 1]).reset_index()

    # Merge the adjustement factors
    df = pd.merge(df_afc, df_adj, on='ticker')

    # Filter data btw start and end dates, then compute unadjusted estimates
    # Filter data for forecast period end dates
    # between start date(included) and end date(not included)
    start_date = ['startdt']
    end_date = ['enddt']
    df = df[(df['fpedats'] >= df[start_date]) & (
            df['fpedats'] < df[end_date])]
    # Complete the missing ones
    df['adj'] = df['adj'].fillna(1)
    # Compute the unadjusted estimates and eps
    df['uest'] = df['est'] * df['adj']
    df['ueps'] = df['eps'] * df['adj']

    return df['ticker', 'fpedats', 'startdt', 'enddt', 'est', 'ueps']


def merge_consensus_stats(w, file_ibes_consesus, file_ibes_stats, columns):
    """Merge the consensus and the statistics"""
    # Add the necessary columns to process the data
    cols_req = ['ticker', 'fpedats']
    cols = cols_req + list(set(columns) - set(cols_req))

    # Open data
    df_ibes_consesus = w.open_data(file_ibes_consesus, cols)
    df_ibes_stats = w.open_data(file_ibes_stats, cols)

    # Merge the two datasets on 'itic' and 'fpedats'
    df_ibes = pd.merge(df_ibes_consesus, df_ibes_stats, on=['itic', 'fpedats'])

    return df_ibes
