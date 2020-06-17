"""
Provides processing functions for IBES data
"""


def _adjustmentfactors(w, file_adj):
    """ Create an adjustment factor table with a start and end date
    for each factor."""
    None


# Create a statistics table that includes:
# -numanalys    Number of distinct analysts by firm-forecast period

def numanalys(file_det):
    """Return the number of analysts for each (itic, fpedats)."""
    # Open the det file with the necessary fields
    cols = ['ticker', 'fpedats', 'analys']
    det = w.open_data(file_det, cols)
    # Count number of analysis
    df = det.groupby(['ticker', 'fpedats'])['analys'].nunique().reset_index()
    df.rename(columns={'analys': 'numanalys'}, inplace=True)

    return df


def consensus(file_det, file_adjfactors=None, file_adj=None):
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
                                                            'Y']))].\
        reset_index(drop=True)
    det = det.dropna(subset=['actual'])
    det['fpeyr'] = det['fpedats'].dt.year
    det['fpemon'] = det['fpedats'].dt.month

    """Fetch the management forecast data"""
    # DOES THIS REQUIRE ANOTHER FILE_DET???
    # Add the necessary columns to process the data
    cols_req = key + ['usfirm', 'curr', 'pdicity', 'measure', 'units']
    cols = cols_req + list(set(columns) - set(cols_req))
    # Open ibes data
    df = w.open_data(file, cols)
    # Filter the ibes data
    df = df[(df.usfirm == 1) & (df.curr == 'USD') &
            (df.pdicity == 'QTR') & (df.measure == 'EPS') & (df.units == 'P/S')]

    df['mftims'] = pd.to_datetime(df['anntims'], unit='s').dt.time

    """Fetch the adjustment factors"""  # USE _adjustmentfactors FUNCTION!!
    # Filter the adjustment data
    adjfactors = adjfactors[(adjfactors['usfirm'] == 1)].reset_index(drop=True)

    """Keep the diluted EPS analysts estimates or the primary
    if no diluted available"""
    for i in range(0, len(df_a_forecast)):
        if df_a_forecast['pdf'][i] == 'D':
            continue
        elif df_a_forecast['pdf'][i] == 'P':
            continue
    else:
        df_a_forecast.drop(df_a_forecast.index[i])

    # Keep consistency with SQL code(will rename later)
    df_a1 = df_a_forecast

    # Drop observations where no announcment dates found
    # THESE MAY BE REDUNDANT(NEED TO CHECK)!!!
    df_a1 = df_a1.dropna(subset=['anndats'])
    # Drop if diff btw announcement dates and forecast period dates > 120 days
    df_a2['interval'] = (df_a1['anndats'] - df_a1['fpedats'])
    df_a2['interval'] = df_a2[df_a2['interval'] / np.timedelta64(1, 'D') <= 120]

    """Remove observations where the period btw FPE and EA is more than 120 days 
    and that do not have EA date."""
    # Get the last 2 earnings annoucement dates
    # NEED TO GET INDEX AND GET LAST ANNOUNCEMENT DATES
    # CAN SET MAXPERIOD AS VARIABLE (MAXPERIOD = 120 DAYS)
    df_ann1 = df.sort_values(by=['ticker', 'fpedats'],
                             ascending=[1, 1]).reset_index()
    df_ann1['anndats_l'] = ''
    for i in range(0, len(df_ann1)):
        df_ann1['anndats_l'][i] = df_ann1['anndats'][i - 1]
    df_ann1['anndats_l'] = pd.to_datetime(df_ann1['anndats_l']).dt.floor('d')
    # Calculate the days between current and last announcement dates
    # Name column as interval_l
    df_ann1['interval_l'] = ''
    df_ann1['interval_l'] = df_ann1['anndats'] - df_ann1['anndats_l']
    df_ann1['interval_l'] = df_ann1[
        df_ann1['interval_l'] / np.timedelta64(1, 'D') <= 120]
    # If interval_l >= 120 days, then replace last announcement date
    # with (current announcement date-120 days)
    for i in range(0, len(df_ann1)):
        if df_ann1['interval'][i] < 120:
            continue
        else:
            df_ann1['anndats_l'][i] = df_ann1['anndats'][i] - 120
    # Keep consistency with SQL code(will rename later)
    df_ann2 = df_ann1

    # Last-last announcements dates
    df_ann3 = df_ann2.sort_values(by=['ticker', 'fpedats'],
                                  ascending=[1, 1]).reset_index()
    df_ann3['anndats_ll'] = ''
    for i in range(0, len(df_ann3)):
        df_ann1['anndats_ll'][i] = df_ann3['anndats_l'][i - 1]
    df_ann3['anndats_ll'] = pd.to_datetime(df_ann3['anndats_l']).dt.floor('d')
    # Calculate the days between last and last-last announcement dates
    # Name column as interval_ll
    df_ann3['interval_ll'] = ''
    df_ann3['interval_ll'] = df_ann1['anndats'] - df_ann1['anndats_l']
    df_ann3['interval_ll'] = df_ann1[
        df_ann1['interval_ll'] / np.timedelta64(1, 'D') <= 120]
    # If interval_ll >= 120 days, then replace last-last announcement date
    # with (last announcement date-120 days)
    for i in range(0, len(df_ann3)):
        if df_ann3['interval_ll'][i] < 120:
            continue
        else:
            df_ann1['anndats_ll'][i] = df_ann1['anndats_l'][i] - 120
    # Keep consistency with SQL code(will rename later)
    df_ann4 = df_df_ann3

    # Merge with analysts estimates
    df_a3 = pd.merge(df_a2, d_ann4, on=['ticker', 'fpedats'])

    """Keep MF(t) between EA(t-1) and FPE(t)"""
    # Merge management forecasts with analysts estimates
    # Keep MF(t) between EA(t-1) and FPE(t)
    df_mf1 = pd.merge(d_m_forecast, d_a3, on=['ticker', 'fpeyr', 'fpemon'])
    # Drop duplicates on ticker(itic), fpeyr, fpemon, anndats_l, fpedats
    df_mf1.drop_duplicates(
        subset=['ticker', 'fpeyr', 'fpemon', 'anndats_l', 'fpedats'])
    # Filter data for management forecasts dates between
    # last announcement dates(included) and forecast period end date(included)
    df_mf1 = df_mf1[(df['mfdats'] >= df_mf1['anndats_l']) & (
            df_mf1['mfdats'] <= df_mf1['fpedats'])]
    # Order by ticker(itic), fpeyr, fpemon, mfdats
    df_mf1 = df_mf1.sort_values(by=['ticker', 'fpeyr', 'fpemon', 'mfdats'],
                                ascending=[1, 1, 1, 1]).reset_index()
    # Drop duplicates on ticker(itic), fpeyr, fpemon
    df_mf2 = df_mf1.drop_duplicates(subset=['ticker', 'fpeyr', 'fpemon'])

    # Merge the analysts estimates and management forecast data
    df_amf1 = pd.merge(df_a3, df_mf2,
                       on=['ticker', 'fpedats', 'fpeyr', 'fpemon', 'anndats_l'])

    # Merge the adjustement factors
    df_amf2 = pd.merge(df_amf1, df_adj_factors, on='ticker')

    # Filter data for forecast period end dates
    # between start date(included) and end date(not included)
    df_amf2 = df_amf2[(df_amf2['fpedats'] >= df_amf2['startdt']) & (
            df_amf2['fpedats'] < df_amf2['enddt'])]

    # Complete the missing ones
    df_amf2['adj'] = df_amf2['adj'].fillna(1)

    # Compute the unadjusted estimates and eps
    df_amf3['uest'] = df_amf2['est'] * df_amf2['adj']
    df_amf3['ueps'] = df_amf2['eps'] * df_amf2['adj']

    """Find the Ex-Post and Ex-ante consensus
    depending on disclosure or no-disclosure"""
    # USE _expd and _expnd FUNCTIONS(WILL BE CREATED)!!!

    """Merge dataframes to create consensus"""
    # Round mean estimates to 3 decimal places and rename columns
    df_expost = df_expost.round({'meanest': 3, 'medest': 3})
    df_expost.rename(
        columns={'meansest': 'meanest_exp', 'medest': 'medest_exp'},
        inplace=True)
    df_exante1 = df_exante1.round({'meanest': 3, 'medest': 3})
    df_exante1.rename(
        columns={'meansest': 'meanest_exa1', 'medest': 'medest_exa1'},
        inplace=True)
    df_exante2 = df_exante2.round({'meanest': 3, 'medest': 3})
    df_exante2.rename(
        columns={'meansest': 'meanest_exa2', 'medest': 'medest_exa2'},
        inplace=True)

    # Merge unadjusted consensus with unadjusted estimates and eps
    df = pd.merge(df_amf3, df_expost, on=['ticker', 'fpedats'])
    df = pd.merge(df, df_exante1, on=['ticker', 'fpedats'])
    df = pd.merge(df, df_exante2, on=['ticker', 'fpedats'])

    return df


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
